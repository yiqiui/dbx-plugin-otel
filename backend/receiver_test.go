package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "otel"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

func demoTraceRequest() *coltrace.ExportTraceServiceRequest {
	traceID, _ := hex.DecodeString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	parentID, _ := hex.DecodeString("0000000000000001")
	childID, _ := hex.DecodeString("0000000000000002")
	start := uint64(time.Now().Add(-time.Second).UnixNano())
	return &coltrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: traceResource("svc-orders"),
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{
					{
						TraceId: traceID, SpanId: parentID, Name: "GET /orders",
						Kind:              tracepb.Span_SPAN_KIND_SERVER,
						StartTimeUnixNano: start, EndTimeUnixNano: start + uint64(120*time.Millisecond),
						Attributes: []*commonpb.KeyValue{
							{Key: "http.route", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "/orders"}}},
							{Key: "http.status_code", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 200}}},
						},
						Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
					},
					{
						TraceId: traceID, SpanId: childID, ParentSpanId: parentID, Name: "SELECT orders",
						Kind:              tracepb.Span_SPAN_KIND_CLIENT,
						StartTimeUnixNano: start + uint64(20*time.Millisecond), EndTimeUnixNano: start + uint64(90*time.Millisecond),
						Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "connection reset"},
					},
				},
			}},
		}},
	}
}

func traceResource(service string) *resourcepb.Resource {
	return &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{{
			Key:   "service.name",
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: service}},
		}},
	}
}

func post(t *testing.T, url string, contentType string, body []byte, headers map[string]string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set("Content-Type", contentType)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func TestReceiverAcceptsProtobufAndJSON(t *testing.T) {
	store := newTestStore(t)
	receiver := NewReceiver(store, "")
	if err := receiver.Start("127.0.0.1", 0); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer receiver.Stop()
	base := "http://" + strings.TrimPrefix(receiver.Status()["endpoint"].(string), "http://")

	protobuf, err := proto.Marshal(demoTraceRequest())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if response := post(t, base+"/v1/traces", "application/x-protobuf", protobuf, nil); response.StatusCode != http.StatusOK {
		t.Fatalf("protobuf ingest status = %d", response.StatusCode)
	}

	jsonPayload, err := protojson.Marshal(demoTraceRequest())
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	if response := post(t, base+"/v1/traces", "application/json", jsonPayload, nil); response.StatusCode != http.StatusOK {
		t.Fatalf("json ingest status = %d", response.StatusCode)
	}

	counts := store.Counts()
	if counts["spans"] != 4 {
		t.Fatalf("expected 4 stored spans, got %d", counts["spans"])
	}

	traces, err := store.ListTraces(traceFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list traces: %v", err)
	}
	if len(traces) != 1 {
		t.Fatalf("expected 1 aggregated trace, got %d", len(traces))
	}
	trace := traces[0]
	if trace["root_name"] != "GET /orders" {
		t.Fatalf("root name = %v", trace["root_name"])
	}
	if trace["span_count"] != 4 {
		t.Fatalf("span count = %v", trace["span_count"])
	}
	if trace["error_count"] != 2 {
		t.Fatalf("error count = %v", trace["error_count"])
	}
	if trace["service"] != "svc-orders" {
		t.Fatalf("service = %v", trace["service"])
	}

	spans, err := store.GetTrace(trace["trace_id"].(string))
	if err != nil {
		t.Fatalf("get trace: %v", err)
	}
	var child map[string]any
	for _, span := range spans {
		if span["name"] == "SELECT orders" && span["parent_span_id"] != "" {
			child = span
			break
		}
	}
	if child == nil {
		t.Fatalf("child span not found among %d spans", len(spans))
	}
	if child["status_code"] != "error" || child["status_message"] != "connection reset" {
		t.Fatalf("child status = %v / %v", child["status_code"], child["status_message"])
	}
	if child["kind"] != "client" {
		t.Fatalf("child kind = %v", child["kind"])
	}

	// Both persistence layers must exist: JSONL for the plugin, CSV for DuckDB.
	files, err := store.FilesOnDisk()
	if err != nil || len(files) == 0 {
		t.Fatalf("expected CSV files on disk, got %v (%v)", files, err)
	}
	payload, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	header := strings.SplitN(string(payload), "\n", 2)[0]
	for _, column := range []string{"trace_id", "span_id", "parent_span_id", "duration_ns", "attributes_json"} {
		if !strings.Contains(header, column) {
			t.Fatalf("csv header missing %q: %s", column, header)
		}
	}
	if !strings.Contains(string(payload), "GET /orders") {
		t.Fatalf("csv body missing span name")
	}

	sql := store.AnalysisSQL()
	if glob, _ := sql["csvGlob"].(map[string]string); glob["spans"] == "" {
		t.Fatalf("analysis SQL has no spans glob")
	}
}

func TestReceiverFallsBackWhenPortBusy(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer blocker.Close()
	busyPort := blocker.Addr().(*net.TCPAddr).Port

	store := newTestStore(t)
	receiver := NewReceiver(store, "")
	if err := receiver.Start("127.0.0.1", busyPort); err != nil {
		t.Fatalf("start should fall back, got %v", err)
	}
	defer receiver.Stop()
	status := receiver.Status()
	if status["fallbackUsed"] != true {
		t.Fatalf("expected fallbackUsed=true, got %#v", status)
	}
	endpoint := strings.TrimPrefix(status["endpoint"].(string), "http://")
	if strings.HasSuffix(endpoint, fmt.Sprintf(":%d", busyPort)) {
		t.Fatalf("receiver claims it bound the busy port %d", busyPort)
	}
}

func TestReceiverRejectsMissingToken(t *testing.T) {
	store := newTestStore(t)
	receiver := NewReceiver(store, "s3cret")
	if err := receiver.Start("127.0.0.1", 0); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer receiver.Stop()
	base := strings.TrimSuffix(receiver.Status()["endpoint"].(string), "")
	protobuf, _ := proto.Marshal(demoTraceRequest())

	if response := post(t, base+"/v1/traces", "application/x-protobuf", protobuf, nil); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", response.StatusCode)
	}
	if response := post(t, base+"/v1/traces", "application/x-protobuf", protobuf, map[string]string{"Authorization": "Bearer s3cret"}); response.StatusCode != http.StatusOK {
		t.Fatalf("authenticated status = %d", response.StatusCode)
	}
	if store.Counts()["spans"] != 2 {
		t.Fatalf("only the authenticated request should have been stored: %#v", store.Counts())
	}
}

func TestReceiverRejectsGarbageBody(t *testing.T) {
	store := newTestStore(t)
	receiver := NewReceiver(store, "")
	if err := receiver.Start("127.0.0.1", 0); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer receiver.Stop()
	response := post(t, receiver.Status()["endpoint"].(string)+"/v1/traces", "application/x-protobuf", []byte{0xff, 0xff, 0xff}, nil)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed payload status = %d", response.StatusCode)
	}
}

func TestSampleAndRetention(t *testing.T) {
	store := newTestStore(t)
	if count := store.IngestSample(); count != 4 {
		t.Fatalf("sample spans = %d", count)
	}
	traces, err := store.ListTraces(traceFilter{Limit: 10, Service: "demo-checkout"})
	if err != nil || len(traces) != 1 {
		t.Fatalf("sample trace not listed: %v (%v)", traces, err)
	}
	// A file dated in the past must be removed by retention.
	oldPath := filepath.Join(store.root, "csv", "spans", "2020-01-01.csv")
	if err := os.WriteFile(oldPath, []byte("stale\n"), 0o600); err != nil {
		t.Fatalf("write stale: %v", err)
	}
	removed, err := store.Purge(7)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed == 0 {
		t.Fatalf("purge removed nothing")
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("stale file survived retention")
	}
}
