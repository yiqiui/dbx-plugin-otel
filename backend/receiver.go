package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const maxRequestBytes = 8 << 20

// Receiver is an OTLP/HTTP ingest endpoint. It runs inside the plugin sidecar
// because the DBX workbench sandbox has no network access at all: telemetry can
// only arrive through a native process.
type Receiver struct {
	store  *Store
	token  string
	server *http.Server

	mu            sync.Mutex
	listener      net.Listener
	requested     string
	endpoint      string
	fallbackUsed  bool
	grpcServer    *grpcServerHandle
	grpcListener  net.Listener
	grpcEndpoint  string
	grpcFallback  bool
	grpcRequested string
	startedAt     time.Time
	lastError     string
	ingested      map[string]int64
	onIngest      func(signal string, count int, endpoint string)
}

func NewReceiver(store *Store, token string) *Receiver {
	return &Receiver{store: store, token: strings.TrimSpace(token), ingested: map[string]int64{}}
}

// setToken updates the ingest credential without racing the request handlers.
func (receiver *Receiver) setToken(token string) {
	receiver.mu.Lock()
	receiver.token = strings.TrimSpace(token)
	receiver.mu.Unlock()
}

// probeListen checks that a preferred endpoint is usable without touching a
// running receiver: it binds, reads back the address, and releases it.
func probeListen(host string, port int) (net.Listener, error) {
	if host == "" {
		host = "127.0.0.1"
	}
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err == nil {
		return listener, nil
	}
	// A fallback bind proves the host is reachable even when the exact port is
	// already taken by another collector.
	return net.Listen("tcp", host+":0")
}

// Start binds the HTTP receiver only (used by tests and simple setups).
func (receiver *Receiver) Start(host string, port int) error {
	return receiver.StartWithGRPC(host, port, 0)
}

// StartWithGRPC binds the listeners. When a preferred port is taken (4318 and
// 4317 are machine-wide shared resources and a local collector often owns them)
// it retries on an ephemeral port instead of failing, and reports fallbackUsed
// so the UI can say "the endpoint you configured is not the one in use".
func (receiver *Receiver) StartWithGRPC(host string, httpPort int, grpcPort int) error {
	if host == "" {
		host = "127.0.0.1"
	}
	receiver.mu.Lock()
	alreadyRunning := receiver.server != nil
	receiver.mu.Unlock()
	if alreadyRunning {
		return nil
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, httpPort))
	fallback := false
	if err != nil {
		if httpPort == 0 {
			receiver.mu.Lock()
			receiver.lastError = err.Error()
			receiver.mu.Unlock()
			return err
		}
		listener, err = net.Listen("tcp", host+":0")
		if err != nil {
			receiver.mu.Lock()
			receiver.lastError = err.Error()
			receiver.mu.Unlock()
			return err
		}
		fallback = true
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", receiver.handleSignal("spans"))
	mux.HandleFunc("/v1/metrics", receiver.handleSignal("metrics"))
	mux.HandleFunc("/v1/logs", receiver.handleSignal("logs"))
	mux.HandleFunc("/health", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ok"}`))
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	receiver.mu.Lock()
	receiver.listener = listener
	receiver.requested = fmt.Sprintf("%s:%d", host, httpPort)
	receiver.endpoint = listener.Addr().String()
	receiver.fallbackUsed = fallback
	receiver.startedAt = time.Now().UTC()
	receiver.lastError = ""
	receiver.server = server
	receiver.mu.Unlock()

	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			receiver.mu.Lock()
			receiver.lastError = serveErr.Error()
			receiver.mu.Unlock()
		}
	}()

	if grpcPort > 0 {
		if _, _, grpcErr := receiver.startGRPC(host, grpcPort); grpcErr != nil {
			// gRPC is an extra surface; failing it must not take the HTTP receiver down.
			log.Printf("otel plugin: grpc receiver unavailable: %v", grpcErr)
		}
	}
	return nil
}

// noteIngested records an accepted batch and forwards a throttled live event.
func (receiver *Receiver) noteIngested(signal string, count int) {
	if count == 0 {
		return
	}
	receiver.mu.Lock()
	receiver.ingested[signal] += int64(count)
	endpoint := receiver.endpoint
	callback := receiver.onIngest
	receiver.mu.Unlock()
	if callback != nil {
		callback(signal, count, endpoint)
	}
}

func (receiver *Receiver) Stop() {
	receiver.stopGRPC()
	receiver.mu.Lock()
	server := receiver.server
	listener := receiver.listener
	receiver.server = nil
	receiver.listener = nil
	receiver.endpoint = ""
	receiver.mu.Unlock()
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
	if listener != nil {
		_ = listener.Close()
	}
}

func (receiver *Receiver) Status() map[string]any {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	listening := receiver.server != nil
	endpoint := receiver.endpoint
	if listening && endpoint != "" {
		endpoint = "http://" + endpoint
	}
	grpcEndpoint := ""
	if receiver.grpcEndpoint != "" {
		grpcEndpoint = "grpc://" + receiver.grpcEndpoint
	}
	return map[string]any{
		"listening":     listening,
		"endpoint":      endpoint,
		"requested":     receiver.requested,
		"fallbackUsed":  receiver.fallbackUsed,
		"grpcListening": receiver.grpcServer != nil,
		"grpcEndpoint":  grpcEndpoint,
		"grpcRequested": receiver.grpcRequested,
		"grpcFallback":  receiver.grpcFallback,
		"startedAt":     receiver.startedAt.Format(time.RFC3339),
		"lastError":     receiver.lastError,
		"authRequired":  receiver.token != "",
		"counts":        receiver.store.Counts(),
		"signalTotals":  copyMap(receiver.ingested),
		"dataDir":       receiver.store.Root(),
		"otlpSupported": []string{"application/x-protobuf", "application/json", "grpc"},
	}
}

func copyMap(source map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func (receiver *Receiver) handleSignal(signal string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(writer, `{"code":3,"message":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		if !receiver.authorized(request) {
			http.Error(writer, `{"code":16,"message":"unauthenticated"}`, http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(io.LimitReader(request.Body, maxRequestBytes+1))
		if err != nil {
			http.Error(writer, `{"code":3,"message":"cannot read body"}`, http.StatusBadRequest)
			return
		}
		if len(body) > maxRequestBytes {
			http.Error(writer, `{"code":3,"message":"payload too large"}`, http.StatusRequestEntityTooLarge)
			return
		}
		defer request.Body.Close()

		isJSON := strings.Contains(request.Header.Get("Content-Type"), "json")
		var count int
		switch signal {
		case "spans":
			message := &coltrace.ExportTraceServiceRequest{}
			if err := decode(body, isJSON, message); err != nil {
				writeDecodeError(writer, err)
				return
			}
			rows := flattenSpans(message.ResourceSpans)
			count = len(rows)
			if err := receiver.store.AppendSpans(rows); err != nil {
				http.Error(writer, `{"code":13,"message":"persist failed"}`, http.StatusInternalServerError)
				return
			}
		case "metrics":
			message := &colmetric.ExportMetricsServiceRequest{}
			if err := decode(body, isJSON, message); err != nil {
				writeDecodeError(writer, err)
				return
			}
			rows := flattenMetrics(message.ResourceMetrics)
			count = len(rows)
			if err := receiver.store.AppendMetrics(rows); err != nil {
				http.Error(writer, `{"code":13,"message":"persist failed"}`, http.StatusInternalServerError)
				return
			}
		case "logs":
			message := &collog.ExportLogsServiceRequest{}
			if err := decode(body, isJSON, message); err != nil {
				writeDecodeError(writer, err)
				return
			}
			rows := flattenLogs(message.ResourceLogs)
			count = len(rows)
			if err := receiver.store.AppendLogs(rows); err != nil {
				http.Error(writer, `{"code":13,"message":"persist failed"}`, http.StatusInternalServerError)
				return
			}
		default:
			http.NotFound(writer, request)
			return
		}

		receiver.mu.Lock()
		receiver.ingested[signal] += int64(count)
		endpoint := receiver.endpoint
		callback := receiver.onIngest
		receiver.mu.Unlock()
		if callback != nil && count > 0 {
			callback(signal, count, endpoint)
		}

		writer.Header().Set("Content-Type", contentTypeFor(isJSON))
		if isJSON {
			_, _ = writer.Write([]byte("{}"))
			return
		}
		_, _ = writer.Write([]byte{})
	}
}

func contentTypeFor(isJSON bool) string {
	if isJSON {
		return "application/json"
	}
	return "application/x-protobuf"
}

func decode(body []byte, isJSON bool, message proto.Message) error {
	if len(body) == 0 {
		return nil
	}
	if isJSON {
		return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(body, message)
	}
	return proto.Unmarshal(body, message)
}

func writeDecodeError(writer http.ResponseWriter, err error) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"code":    3,
		"message": fmt.Sprintf("cannot decode OTLP request: %v", err),
	})
}

func (receiver *Receiver) authorized(request *http.Request) bool {
	if receiver.token == "" {
		return true
	}
	provided := request.Header.Get("Authorization")
	if provided == "" {
		provided = request.Header.Get("X-OTLP-Token")
	}
	provided = strings.TrimPrefix(provided, "Bearer ")
	return subtle.ConstantTimeCompare([]byte(provided), []byte(receiver.token)) == 1
}
