package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func spanRow(traceID string) SpanRow {
	now := time.Now().UnixNano()
	return SpanRow{
		TraceID: traceID, SpanID: traceID + "-span", Name: "op",
		Service: "svc", StartUnixNano: now, EndUnixNano: now + 1000,
		DurationNs: 1000, StatusCode: "ok",
		Attributes: map[string]any{}, ResourceAttributes: map[string]any{},
	}
}

// A sidecar restart loses the in-memory counters; the reopened store must
// report what is actually stored on disk, not zeros (#0-count status card).
func TestCountsRecountedFromDiskAfterRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "otel")
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.AppendSpans([]SpanRow{spanRow("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), spanRow("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")}); err != nil {
		t.Fatalf("AppendSpans: %v", err)
	}
	if err := store.AppendLogs([]LogRow{{Body: "hello", SeverityText: "INFO"}}); err != nil {
		t.Fatalf("AppendLogs: %v", err)
	}
	store.Close()

	reopened, err := NewStore(root)
	if err != nil {
		t.Fatalf("reopen NewStore: %v", err)
	}
	defer reopened.Close()
	counts := reopened.Counts()
	if counts["spans"] != 2 {
		t.Fatalf("reopened spans count = %d, want 2", counts["spans"])
	}
	if counts["logs"] != 1 {
		t.Fatalf("reopened logs count = %d, want 1", counts["logs"])
	}
	if counts["metrics"] != 0 {
		t.Fatalf("reopened metrics count = %d, want 0", counts["metrics"])
	}
}

// Purge must remove stale files AND keep the counters in sync with the disk,
// including files this process never counted.
func TestPurgeRemovesStaleFilesAndRecounts(t *testing.T) {
	store := newTestStore(t)
	if err := store.AppendSpans([]SpanRow{spanRow("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}); err != nil {
		t.Fatalf("AppendSpans: %v", err)
	}
	staleJSONL := filepath.Join(store.root, "jsonl", "spans", "2020-01-01.jsonl")
	staleCSV := filepath.Join(store.root, "csv", "spans", "2020-01-01.csv")
	staleBody := "{\"trace_id\":\"old\"}\n{\"trace_id\":\"older\"}\n"
	if err := os.WriteFile(staleJSONL, []byte(staleBody), 0o600); err != nil {
		t.Fatalf("write stale jsonl: %v", err)
	}
	if err := os.WriteFile(staleCSV, []byte("stale\n"), 0o600); err != nil {
		t.Fatalf("write stale csv: %v", err)
	}

	removed, err := store.Purge(7)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed != 2 {
		t.Fatalf("purge removed %d files, want 2", removed)
	}
	for _, path := range []string{staleJSONL, staleCSV} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s survived retention", path)
		}
	}
	if counts := store.Counts(); counts["spans"] != 1 {
		t.Fatalf("spans count after purge = %d, want 1", counts["spans"])
	}
}

// Purging an old day must not tear down the current day's writer: appends
// after the purge still land in today's files.
func TestPurgeKeepsCurrentDayWriter(t *testing.T) {
	store := newTestStore(t)
	if err := store.AppendSpans([]SpanRow{spanRow("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}); err != nil {
		t.Fatalf("AppendSpans: %v", err)
	}
	staleJSONL := filepath.Join(store.root, "jsonl", "spans", "2020-01-01.jsonl")
	if err := os.WriteFile(staleJSONL, []byte("{\"trace_id\":\"old\"}\n"), 0o600); err != nil {
		t.Fatalf("write stale jsonl: %v", err)
	}
	if _, err := store.Purge(7); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if err := store.AppendSpans([]SpanRow{spanRow("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")}); err != nil {
		t.Fatalf("append after purge: %v", err)
	}
	traces, err := store.ListTraces(traceFilter{Limit: 10})
	if err != nil {
		t.Fatalf("ListTraces: %v", err)
	}
	if len(traces) != 2 {
		t.Fatalf("traces after purge = %d, want 2 (%v)", len(traces), traces)
	}
	if counts := store.Counts(); counts["spans"] != 2 {
		t.Fatalf("spans count after purge+append = %d, want 2", counts["spans"])
	}
}

// Telemetry records routinely exceed bufio's 4 KiB default token size; the
// line counter must still count them instead of silently stopping early.
func TestCountNonEmptyLinesHandlesLargeRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.jsonl")
	huge := strings.Repeat("x", 1<<20)
	content := "{\"a\":1}\n{\"b\":\"" + huge + "\"}\n\n{\"c\":3}\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := countNonEmptyLines(path); got != 3 {
		t.Fatalf("countNonEmptyLines = %d, want 3", got)
	}
}
