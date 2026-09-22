package main

import (
	"context"
	"errors"
	"strings"
	"testing"
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

// These tests drive the receiver the way a real instrumented application does:
// through the official OpenTelemetry Go SDK and its OTLP exporters, rather than
// hand-built protobuf. Wire-format or resource-semantics drift shows up here.

var errDemo = errors.New("deadlock detected")

func installProvider(t *testing.T, exporter sdktrace.SpanExporter, service string) *sdktrace.TracerProvider {
	t.Helper()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(50*time.Millisecond)),
		sdktrace.WithResource(sdkresource.NewSchemaless(
			attribute.String("service.name", service),
			attribute.String("deployment.environment", "test"),
		)),
	)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return provider
}

// emitCheckoutWork produces the span tree a small service would emit: an HTTP
// server span with a DB child that errors and a cache child that succeeds.
func emitCheckoutWork(t *testing.T, provider *sdktrace.TracerProvider) string {
	t.Helper()
	tracer := provider.Tracer("checkout")
	ctx := context.Background()

	rootCtx, root := tracer.Start(ctx, "POST /checkout", trace.WithSpanKind(trace.SpanKindServer))
	defer root.End()

	_, orderDB := tracer.Start(rootCtx, "SELECT orders", trace.WithSpanKind(trace.SpanKindClient))
	orderDB.RecordError(errDemo)
	orderDB.SetStatus(codes.Error, "deadlock detected")
	orderDB.End()

	_, cache := tracer.Start(rootCtx, "GET cache:pricing", trace.WithSpanKind(trace.SpanKindInternal))
	cache.SetAttributes(attribute.Bool("cache.hit", true))
	cache.End()

	root.SetAttributes(attribute.Int("http.response.status_code", 201))
	traceID := root.SpanContext().TraceID().String()
	if err := provider.ForceFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return traceID
}

func waitForSpans(t *testing.T, store *Store, traceID string, want int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		spans, err := store.GetTrace(traceID)
		if err == nil && len(spans) >= want {
			return spans
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d spans of trace %s", want, traceID)
	return nil
}

func hostPort(endpoint string) string { return strings.TrimPrefix(endpoint, "http://") }

func TestRealSDKExportsOverHTTP(t *testing.T) {
	store := newTestStore(t)
	receiver := NewReceiver(store, "")
	if err := receiver.Start("127.0.0.1", 0); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer receiver.Stop()

	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint(hostPort(receiver.Status()["endpoint"].(string))),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("http exporter: %v", err)
	}
	provider := installProvider(t, exporter, "checkout-http")
	traceID := emitCheckoutWork(t, provider)

	spans := waitForSpans(t, store, traceID, 3)
	if len(spans) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(spans))
	}
	byName := map[string]map[string]any{}
	for _, span := range spans {
		byName[span["name"].(string)] = span
	}
	root, ok := byName["POST /checkout"]
	if !ok {
		t.Fatalf("root span missing: %#v", byName)
	}
	if root["kind"] != "server" {
		t.Fatalf("root kind = %v", root["kind"])
	}
	if root["service"] != "checkout-http" {
		t.Fatalf("resource service.name lost: %v", root["service"])
	}
	db := byName["SELECT orders"]
	if db == nil || db["status_code"] != "error" {
		t.Fatalf("db span status = %#v", db)
	}
	if db["parent_span_id"] != root["span_id"] {
		t.Fatalf("parent link broken: %v vs %v", db["parent_span_id"], root["span_id"])
	}
	cache := byName["GET cache:pricing"]
	if attributes, _ := cache["attributes"].(map[string]any); attributes["cache.hit"] != true {
		t.Fatalf("bool attribute lost: %#v", cache["attributes"])
	}
	if resource, _ := root["resource_attributes"].(map[string]any); resource["deployment.environment"] != "test" {
		t.Fatalf("resource attributes lost: %#v", root["resource_attributes"])
	}

	traces, err := store.ListTraces(traceFilter{Limit: 10, Service: "checkout-http"})
	if err != nil || len(traces) != 1 {
		t.Fatalf("trace not aggregated: %v (%v)", traces, err)
	}
	if traces[0]["error_count"] != 1 {
		t.Fatalf("error count = %v", traces[0]["error_count"])
	}
	if traces[0]["span_count"] != 3 {
		t.Fatalf("span count = %v", traces[0]["span_count"])
	}
}

func TestRealSDKExportsOverGRPC(t *testing.T) {
	store := newTestStore(t)
	receiver := NewReceiver(store, "")
	if err := receiver.Start("127.0.0.1", 0); err != nil {
		t.Fatalf("start http: %v", err)
	}
	defer receiver.Stop()
	endpoint, _, err := receiver.startGRPC("127.0.0.1", 0)
	if err != nil {
		t.Fatalf("start grpc: %v", err)
	}

	exporter, err := otlptracegrpc.New(context.Background(),
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("grpc exporter: %v", err)
	}
	provider := installProvider(t, exporter, "checkout-grpc")
	traceID := emitCheckoutWork(t, provider)

	spans := waitForSpans(t, store, traceID, 3)
	if len(spans) != 3 {
		t.Fatalf("expected 3 spans over grpc, got %d", len(spans))
	}
	for _, span := range spans {
		if span["service"] != "checkout-grpc" {
			t.Fatalf("service lost on %v: %v", span["name"], span["service"])
		}
	}
}
