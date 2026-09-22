package main

import (
	"context"
	"crypto/subtle"
	"log"
	"net"
	"strings"

	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// OTLP/gRPC (default 4317) sits beside the HTTP receiver on the same store, so
// an app can set OTEL_EXPORTER_OTLP_PROTOCOL=grpc without a collector in between.

type grpcContextKey struct{}

// grpcServerHandle aliases the grpc server type so receiver.go can hold the
// field without importing the grpc package.
type grpcServerHandle = grpc.Server

// tokenInterceptor lifts the ingest credential out of request metadata once and
// hands it to the services through the context.
func tokenInterceptor(ctx context.Context, handler any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		candidates := append(md.Get("authorization"), md.Get("x-otlp-token")...)
		for index, value := range candidates {
			candidates[index] = strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
		}
		ctx = context.WithValue(ctx, grpcContextKey{}, strings.Join(candidates, "\n"))
	}
	return next(ctx, handler)
}

type traceService struct {
	coltrace.UnimplementedTraceServiceServer
	receiver *Receiver
}

func (service *traceService) Export(ctx context.Context, request *coltrace.ExportTraceServiceRequest) (*coltrace.ExportTraceServiceResponse, error) {
	if err := service.receiver.authorizeGRPC(ctx); err != nil {
		return nil, err
	}
	rows := flattenSpans(request.GetResourceSpans())
	if err := service.receiver.store.AppendSpans(rows); err != nil {
		return nil, status.Errorf(codes.Internal, "persist failed: %v", err)
	}
	service.receiver.noteIngested("spans", len(rows))
	return &coltrace.ExportTraceServiceResponse{}, nil
}

type metricsService struct {
	colmetric.UnimplementedMetricsServiceServer
	receiver *Receiver
}

func (service *metricsService) Export(ctx context.Context, request *colmetric.ExportMetricsServiceRequest) (*colmetric.ExportMetricsServiceResponse, error) {
	if err := service.receiver.authorizeGRPC(ctx); err != nil {
		return nil, err
	}
	rows := flattenMetrics(request.GetResourceMetrics())
	if err := service.receiver.store.AppendMetrics(rows); err != nil {
		return nil, status.Errorf(codes.Internal, "persist failed: %v", err)
	}
	service.receiver.noteIngested("metrics", len(rows))
	return &colmetric.ExportMetricsServiceResponse{}, nil
}

type logsService struct {
	collog.UnimplementedLogsServiceServer
	receiver *Receiver
}

func (service *logsService) Export(ctx context.Context, request *collog.ExportLogsServiceRequest) (*collog.ExportLogsServiceResponse, error) {
	if err := service.receiver.authorizeGRPC(ctx); err != nil {
		return nil, err
	}
	rows := flattenLogs(request.GetResourceLogs())
	if err := service.receiver.store.AppendLogs(rows); err != nil {
		return nil, status.Errorf(codes.Internal, "persist failed: %v", err)
	}
	service.receiver.noteIngested("logs", len(rows))
	return &collog.ExportLogsServiceResponse{}, nil
}

// authorizeGRPC mirrors the HTTP receiver's token check.
func (receiver *Receiver) authorizeGRPC(ctx context.Context) error {
	receiver.mu.Lock()
	expected := receiver.token
	receiver.mu.Unlock()
	if expected == "" {
		return nil
	}
	provided, _ := ctx.Value(grpcContextKey{}).(string)
	for _, candidate := range strings.Split(provided, "\n") {
		if subtle.ConstantTimeCompare([]byte(candidate), []byte(expected)) == 1 {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "invalid or missing ingest token")
}

// startGRPC binds the gRPC listener and registers all three OTLP services.
func (receiver *Receiver) startGRPC(host string, port int) (string, bool, error) {
	if host == "" {
		host = "127.0.0.1"
	}
	requested := net.JoinHostPort(host, itoa(port))
	listener, err := net.Listen("tcp", requested)
	fallback := false
	if err != nil {
		listener, err = net.Listen("tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			return "", false, err
		}
		fallback = true
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(tokenInterceptor))
	coltrace.RegisterTraceServiceServer(server, &traceService{receiver: receiver})
	colmetric.RegisterMetricsServiceServer(server, &metricsService{receiver: receiver})
	collog.RegisterLogsServiceServer(server, &logsService{receiver: receiver})

	receiver.mu.Lock()
	receiver.grpcServer = server
	receiver.grpcListener = listener
	receiver.grpcEndpoint = listener.Addr().String()
	receiver.grpcFallback = fallback
	receiver.grpcRequested = requested
	receiver.mu.Unlock()

	go func() {
		if serveErr := server.Serve(listener); serveErr != nil {
			log.Printf("otel plugin: grpc serve stopped: %v", serveErr)
		}
	}()
	return receiver.grpcEndpoint, fallback, nil
}

func (receiver *Receiver) stopGRPC() {
	receiver.mu.Lock()
	server := receiver.grpcServer
	listener := receiver.grpcListener
	receiver.grpcServer = nil
	receiver.grpcListener = nil
	receiver.grpcEndpoint = ""
	receiver.mu.Unlock()
	if server != nil {
		server.Stop()
	}
	if listener != nil {
		_ = listener.Close()
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var digits []byte
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	if negative {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}
