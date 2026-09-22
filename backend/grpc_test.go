package main

import (
	"context"
	"testing"
	"time"

	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func dialTraceClient(t *testing.T, endpoint string) coltrace.TraceServiceClient {
	t.Helper()
	connection, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { connection.Close() })
	return coltrace.NewTraceServiceClient(connection)
}

func exportWithTimeout(t *testing.T, client coltrace.TraceServiceClient, headers map[string]string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if len(headers) > 0 {
		values := metadata.MD{}
		for key, value := range headers {
			values.Set(key, value)
		}
		ctx = metadata.NewOutgoingContext(ctx, values)
	}
	_, err := client.Export(ctx, demoTraceRequest())
	return err
}

func TestGRPCReceiverExportsSpans(t *testing.T) {
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

	if err := exportWithTimeout(t, dialTraceClient(t, endpoint), nil); err != nil {
		t.Fatalf("grpc export: %v", err)
	}
	if got := store.Counts()["spans"]; got != 2 {
		t.Fatalf("expected 2 spans via grpc, got %d", got)
	}
	status := receiver.Status()
	if status["grpcListening"] != true {
		t.Fatalf("status should report grpc listening: %#v", status)
	}
	if status["grpcEndpoint"] == "" {
		t.Fatalf("status should expose the grpc endpoint")
	}

	// The same trace must be queryable through the shared store.
	traces, err := store.ListTraces(traceFilter{Limit: 10})
	if err != nil || len(traces) != 1 {
		t.Fatalf("grpc-ingested trace not listed: %v (%v)", traces, err)
	}
	if traces[0]["root_name"] != "GET /orders" {
		t.Fatalf("root name = %v", traces[0]["root_name"])
	}
}

func TestGRPCReceiverEnforcesToken(t *testing.T) {
	store := newTestStore(t)
	receiver := NewReceiver(store, "s3cret")
	if err := receiver.Start("127.0.0.1", 0); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer receiver.Stop()
	endpoint, _, err := receiver.startGRPC("127.0.0.1", 0)
	if err != nil {
		t.Fatalf("start grpc: %v", err)
	}
	client := dialTraceClient(t, endpoint)

	if err := exportWithTimeout(t, client, nil); err == nil {
		t.Fatalf("export without a token should fail")
	}
	if store.Counts()["spans"] != 0 {
		t.Fatalf("rejected export must not persist spans")
	}
	if err := exportWithTimeout(t, client, map[string]string{"authorization": "Bearer s3cret"}); err != nil {
		t.Fatalf("authorized export failed: %v", err)
	}
	if store.Counts()["spans"] != 2 {
		t.Fatalf("authorized export should persist 2 spans, got %d", store.Counts()["spans"])
	}
}
