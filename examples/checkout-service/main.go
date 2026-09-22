// Command checkout-service emits real OTLP telemetry through the official
// OpenTelemetry Go SDK, for verifying the DBX plugin end to end.
//
//	go run .                                  # http/protobuf -> 127.0.0.1:4318
//	OTEL_EXPORTER_OTLP_PROTOCOL=grpc go run . # grpc          -> 127.0.0.1:4317
//	OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:54321 go run .
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	ctx := context.Background()
	endpoint := firstNonEmpty(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "http://127.0.0.1:4318")
	protocol := firstNonEmpty(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"), "http/protobuf")

	exporter, err := newExporter(ctx, endpoint, protocol)
	if err != nil {
		log.Fatalf("exporter: %v", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(sdkresource.NewSchemaless(
			attribute.String("service.name", "checkout-service"),
			attribute.String("deployment.environment", "local"),
		)),
	)
	otel.SetTracerProvider(provider)
	defer func() { _ = provider.Shutdown(ctx) }()

	fmt.Printf("exporting OTLP via %s to %s\n", protocol, endpoint)
	tracer := provider.Tracer("checkout")

	for request := 1; request <= 5; request++ {
		rootCtx, root := tracer.Start(ctx, "POST /checkout", trace.WithSpanKind(trace.SpanKindServer))
		root.SetAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("url.path", "/checkout"),
		)

		_, validate := tracer.Start(rootCtx, "cart.validate", trace.WithSpanKind(trace.SpanKindInternal))
		validate.SetAttributes(attribute.Int("cart.items", rand.Intn(5)+1))
		time.Sleep(time.Duration(5+rand.Intn(20)) * time.Millisecond)
		validate.End()

		// One in five requests fails in the payment query so error filtering is visible.
		if request%5 == 0 {
			_, db := tracer.Start(rootCtx, "SELECT payments", trace.WithSpanKind(trace.SpanKindClient))
			db.RecordError(errors.New("deadlock detected"))
			db.SetStatus(codes.Error, "deadlock detected")
			db.SetAttributes(attribute.String("db.system", "postgresql"))
			db.End()
			root.SetStatus(codes.Error, "checkout failed")
		} else {
			_, db := tracer.Start(rootCtx, "SELECT payments", trace.WithSpanKind(trace.SpanKindClient))
			time.Sleep(time.Duration(20+rand.Intn(300)) * time.Millisecond)
			db.End()
		}

		_, publish := tracer.Start(rootCtx, "kafka.publish order.created", trace.WithSpanKind(trace.SpanKindProducer))
		publish.SetAttributes(attribute.String("messaging.system", "kafka"))
		publish.End()

		root.SetAttributes(attribute.Int("http.response.status_code", httpStatus(request)))
		root.End()
	}

	if err := provider.ForceFlush(ctx); err != nil {
		log.Fatalf("flush: %v", err)
	}
	fmt.Println("done — open the OTel 浏览器 workbench in DBX and refresh")
}

func newExporter(ctx context.Context, endpoint, protocol string) (sdktrace.SpanExporter, error) {
	if strings.HasPrefix(protocol, "grpc") {
		hostPort := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
		return otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(hostPort), otlptracegrpc.WithInsecure())
	}
	// WithEndpointURL uses the URL path verbatim, so an endpoint without a path
	// would POST to "/" and get a 404. Default to the OTLP trace signal path.
	url := strings.TrimRight(endpoint, "/")
	if !strings.Contains(url[strings.Index(url, "://")+3:], "/") {
		url += "/v1/traces"
	}
	return otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(url))
}

func httpStatus(request int) int {
	if request%5 == 0 {
		return 500
	}
	return 201
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
