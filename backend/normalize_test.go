package main

import "testing"

// summarize() must pick the root from the parent graph, not from slice order:
// batched exporters deliver spans in arbitrary order, and a wrong root makes
// the whole trace list misleading.
func TestSummarizeRootIgnoresInputOrder(t *testing.T) {
	rows := []SpanRow{
		{TraceID: "t1", SpanID: "s2", ParentSpanID: "s1", Name: "SELECT orders", Service: "db", StartUnixNano: 20, EndUnixNano: 30, DurationNs: 10},
		{TraceID: "t1", SpanID: "s3", ParentSpanID: "s1", Name: "GET cache", Service: "cache", StartUnixNano: 25, EndUnixNano: 28, DurationNs: 3},
		{TraceID: "t1", SpanID: "s1", ParentSpanID: "", Name: "POST /checkout", Service: "web", StartUnixNano: 10, EndUnixNano: 40, DurationNs: 30},
	}
	summaries := summarize(map[string][]SpanRow{"t1": rows})
	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary, got %d", len(summaries))
	}
	got := summaries[0]
	if got.RootName != "POST /checkout" {
		t.Fatalf("root name = %q, want POST /checkout", got.RootName)
	}
	if got.Service != "web" {
		t.Fatalf("root service = %q, want web", got.Service)
	}
	if got.StartNano != 10 || got.EndNano != 40 || got.DurationNs != 30 {
		t.Fatalf("window = %d..%d (%d)", got.StartNano, got.EndNano, got.DurationNs)
	}
	if len(got.Services) != 3 {
		t.Fatalf("expected 3 distinct services, got %v", got.Services)
	}
}

// A trace whose entry span lives in another process arrives with a parent id
// that is not part of this trace; the earliest such orphan is the local root.
func TestSummarizeRootHandlesOrphanParent(t *testing.T) {
	rows := []SpanRow{
		{TraceID: "t2", SpanID: "b", ParentSpanID: "remote-root", Name: "consumer.handle", Service: "worker", StartUnixNano: 5, EndUnixNano: 9},
		{TraceID: "t2", SpanID: "c", ParentSpanID: "b", Name: "child", Service: "worker", StartUnixNano: 6, EndUnixNano: 8},
	}
	got := summarize(map[string][]SpanRow{"t2": rows})[0]
	if got.RootName != "consumer.handle" {
		t.Fatalf("root name = %q, want consumer.handle", got.RootName)
	}
}

func TestSummarizeCountsErrorsAndSlowest(t *testing.T) {
	rows := []SpanRow{
		{TraceID: "t3", SpanID: "a", Name: "root", Service: "web", StartUnixNano: 0, EndUnixNano: 100, DurationNs: 100},
		{TraceID: "t3", SpanID: "b", ParentSpanID: "a", Name: "flaky", StatusCode: "error", StartUnixNano: 10, EndUnixNano: 90, DurationNs: 80},
	}
	got := summarize(map[string][]SpanRow{"t3": rows})[0]
	if got.ErrorCount != 1 {
		t.Fatalf("error count = %d", got.ErrorCount)
	}
	if got.SlowestSpan != "root" || got.MaxDuration != 100 {
		t.Fatalf("slowest = %q (%d)", got.SlowestSpan, got.MaxDuration)
	}
}
