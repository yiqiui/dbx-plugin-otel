package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SpanRow is one flattened OTLP span. The field set is deliberately stable:
// it is both the JSONL schema and the CSV header that DuckDB reads, so adding a
// column means bumping the layout, not silently changing past files.
type SpanRow struct {
	TraceID            string         `json:"trace_id"`
	SpanID             string         `json:"span_id"`
	ParentSpanID       string         `json:"parent_span_id"`
	Name               string         `json:"name"`
	Kind               string         `json:"kind"`
	Service            string         `json:"service"`
	StartUnixNano      int64          `json:"start_unix_nano"`
	EndUnixNano        int64          `json:"end_unix_nano"`
	DurationNs         int64          `json:"duration_ns"`
	StatusCode         string         `json:"status_code"`
	StatusMessage      string         `json:"status_message"`
	Attributes         map[string]any `json:"attributes"`
	ResourceAttributes map[string]any `json:"resource_attributes"`
	Events             []any          `json:"events"`
	Links              []any          `json:"links"`
	ReceivedAtUnixNano int64          `json:"received_at_unix_nano"`
}

type MetricRow struct {
	MetricName         string         `json:"metric_name"`
	Description        string         `json:"description"`
	Unit               string         `json:"unit"`
	Kind               string         `json:"kind"`
	Service            string         `json:"service"`
	Attributes         map[string]any `json:"attributes"`
	ResourceAttributes map[string]any `json:"resource_attributes"`
	StartUnixNano      int64          `json:"start_unix_nano"`
	TimeUnixNano       int64          `json:"time_unix_nano"`
	ValueDouble        float64        `json:"value_double"`
	ValueJSON          any            `json:"value_json"`
	ReceivedAtUnixNano int64          `json:"received_at_unix_nano"`
}

type LogRow struct {
	TimestampUnixNano  int64          `json:"timestamp_unix_nano"`
	SeverityText       string         `json:"severity_text"`
	SeverityNumber     int32          `json:"severity_number"`
	Service            string         `json:"service"`
	Body               string         `json:"body"`
	Attributes         map[string]any `json:"attributes"`
	ResourceAttributes map[string]any `json:"resource_attributes"`
	TraceID            string         `json:"trace_id"`
	SpanID             string         `json:"span_id"`
	ReceivedAtUnixNano int64          `json:"received_at_unix_nano"`
}

var signalColumns = map[string][]string{
	"spans": {
		"trace_id", "span_id", "parent_span_id", "name", "kind", "service",
		"start_unix_nano", "end_unix_nano", "duration_ns", "status_code", "status_message",
		"attributes_json", "resource_attributes_json", "events_json", "links_json", "received_at_unix_nano",
	},
	"metrics": {
		"metric_name", "description", "unit", "kind", "service",
		"attributes_json", "resource_attributes_json",
		"start_unix_nano", "time_unix_nano", "value_double", "value_json", "received_at_unix_nano",
	},
	"logs": {
		"timestamp_unix_nano", "severity_text", "severity_number", "service", "body",
		"attributes_json", "resource_attributes_json", "trace_id", "span_id", "received_at_unix_nano",
	},
}

// Store persists accepted telemetry as per-day JSONL (plugin's own query path)
// and per-day CSV (DuckDB / external analysis path) under the plugin data dir.
type Store struct {
	mu      sync.Mutex
	root    string
	writers map[string]*dayWriter
	counts  map[string]int64
}

type dayWriter struct {
	jsonFile *os.File
	jsonBuf  *bufio.Writer
	csvFile  *os.File
	csvBuf   *bufio.Writer
	csvw     *csv.Writer
}

func NewStore(root string) (*Store, error) {
	for _, signal := range []string{"spans", "metrics", "logs"} {
		if err := os.MkdirAll(filepath.Join(root, "jsonl", signal), 0o700); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Join(root, "csv", signal), 0o700); err != nil {
			return nil, err
		}
	}
	store := &Store{
		root:    root,
		writers: map[string]*dayWriter{},
		counts:  map[string]int64{"spans": 0, "metrics": 0, "logs": 0},
	}
	return store, nil
}

func (store *Store) Root() string { return store.root }

func (store *Store) writerFor(signal string) (*dayWriter, error) {
	day := time.Now().UTC().Format("2006-01-02")
	key := signal + "/" + day
	if existing, ok := store.writers[key]; ok {
		return existing, nil
	}
	jsonPath := filepath.Join(store.root, "jsonl", signal, day+".jsonl")
	csvPath := filepath.Join(store.root, "csv", signal, day+".csv")
	jsonFile, err := os.OpenFile(jsonPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	csvFile, err := os.OpenFile(csvPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		jsonFile.Close()
		return nil, err
	}
	writer := &dayWriter{
		jsonFile: jsonFile,
		jsonBuf:  bufio.NewWriter(jsonFile),
		csvFile:  csvFile,
		csvBuf:   bufio.NewWriter(csvFile),
	}
	writer.csvw = csv.NewWriter(writer.csvBuf)
	// A freshly created CSV must carry the header DuckDB's read_csv_auto relies on.
	if info, statErr := csvFile.Stat(); statErr == nil && info.Size() == 0 {
		if writeErr := writer.csvw.Write(signalColumns[signal]); writeErr != nil {
			jsonFile.Close()
			csvFile.Close()
			return nil, writeErr
		}
		writer.csvw.Flush()
	}
	store.writers[key] = writer
	return writer, nil
}

func (store *Store) AppendSpans(rows []SpanRow) error {
	if len(rows) == 0 {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	writer, err := store.writerFor("spans")
	if err != nil {
		return err
	}
	for index := range rows {
		row := &rows[index]
		if row.ReceivedAtUnixNano == 0 {
			row.ReceivedAtUnixNano = time.Now().UnixNano()
		}
		if row.DurationNs == 0 && row.EndUnixNano > row.StartUnixNano {
			row.DurationNs = row.EndUnixNano - row.StartUnixNano
		}
		if err := writeJSONLine(writer.jsonBuf, row); err != nil {
			return err
		}
		record := []string{
			row.TraceID, row.SpanID, row.ParentSpanID, row.Name, row.Kind, row.Service,
			strconv.FormatInt(row.StartUnixNano, 10), strconv.FormatInt(row.EndUnixNano, 10), strconv.FormatInt(row.DurationNs, 10),
			row.StatusCode, row.StatusMessage,
			jsonString(row.Attributes), jsonString(row.ResourceAttributes),
			jsonString(row.Events), jsonString(row.Links),
			strconv.FormatInt(row.ReceivedAtUnixNano, 10),
		}
		if err := writer.csvw.Write(record); err != nil {
			return err
		}
	}
	writer.csvw.Flush()
	if err := writer.csvw.Error(); err != nil {
		return err
	}
	if err := writer.jsonBuf.Flush(); err != nil {
		return err
	}
	store.counts["spans"] += int64(len(rows))
	return nil
}

func (store *Store) AppendMetrics(rows []MetricRow) error {
	if len(rows) == 0 {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	writer, err := store.writerFor("metrics")
	if err != nil {
		return err
	}
	for index := range rows {
		row := &rows[index]
		if row.ReceivedAtUnixNano == 0 {
			row.ReceivedAtUnixNano = time.Now().UnixNano()
		}
		if err := writeJSONLine(writer.jsonBuf, row); err != nil {
			return err
		}
		record := []string{
			row.MetricName, row.Description, row.Unit, row.Kind, row.Service,
			jsonString(row.Attributes), jsonString(row.ResourceAttributes),
			strconv.FormatInt(row.StartUnixNano, 10), strconv.FormatInt(row.TimeUnixNano, 10),
			strconv.FormatFloat(row.ValueDouble, 'f', -1, 64), jsonString(row.ValueJSON),
			strconv.FormatInt(row.ReceivedAtUnixNano, 10),
		}
		if err := writer.csvw.Write(record); err != nil {
			return err
		}
	}
	writer.csvw.Flush()
	if err := writer.csvw.Error(); err != nil {
		return err
	}
	if err := writer.jsonBuf.Flush(); err != nil {
		return err
	}
	store.counts["metrics"] += int64(len(rows))
	return nil
}

func (store *Store) AppendLogs(rows []LogRow) error {
	if len(rows) == 0 {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	writer, err := store.writerFor("logs")
	if err != nil {
		return err
	}
	for index := range rows {
		row := &rows[index]
		if row.ReceivedAtUnixNano == 0 {
			row.ReceivedAtUnixNano = time.Now().UnixNano()
		}
		if err := writeJSONLine(writer.jsonBuf, row); err != nil {
			return err
		}
		record := []string{
			strconv.FormatInt(row.TimestampUnixNano, 10), row.SeverityText, strconv.Itoa(int(row.SeverityNumber)),
			row.Service, row.Body,
			jsonString(row.Attributes), jsonString(row.ResourceAttributes),
			row.TraceID, row.SpanID, strconv.FormatInt(row.ReceivedAtUnixNano, 10),
		}
		if err := writer.csvw.Write(record); err != nil {
			return err
		}
	}
	writer.csvw.Flush()
	if err := writer.csvw.Error(); err != nil {
		return err
	}
	if err := writer.jsonBuf.Flush(); err != nil {
		return err
	}
	store.counts["logs"] += int64(len(rows))
	return nil
}

func (store *Store) Counts() map[string]int64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	out := make(map[string]int64, len(store.counts))
	for key, value := range store.counts {
		out[key] = value
	}
	return out
}

// FilesOnDisk reports the retained data files, newest first, so the UI can show
// what an external DuckDB session should glob.
func (store *Store) FilesOnDisk() ([]string, error) {
	var files []string
	for _, signal := range []string{"spans", "metrics", "logs"} {
		dir := filepath.Join(store.root, "csv", signal)
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			files = append(files, filepath.ToSlash(filepath.Join(dir, entry.Name())))
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	return files, nil
}

// CSVGlob returns the DuckDB-ready glob for one signal.
func (store *Store) CSVGlob(signal string) string {
	return filepath.ToSlash(filepath.Join(store.root, "csv", signal, "*.csv"))
}

// Purge removes data files older than retentionDays and returns how many were dropped.
func (store *Store) Purge(retentionDays int) (int, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays).Format("2006-01-02")
	store.mu.Lock()
	defer store.mu.Unlock()
	removed := 0
	for _, signal := range []string{"spans", "metrics", "logs"} {
		for _, extension := range []string{".csv", ".jsonl"} {
			dir := filepath.Join(store.root, "jsonl", signal)
			if extension == ".csv" {
				dir = filepath.Join(store.root, "csv", signal)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				name := entry.Name()
				day := strings.TrimSuffix(strings.TrimSuffix(name, extension), ".tmp")
				if entry.IsDir() || len(day) != 10 || day >= cutoff {
					continue
				}
				path := filepath.Join(dir, name)
				// Close any live writer for this day before unlinking it.
				for key, writer := range store.writers {
					if strings.HasPrefix(key, signal+"/") {
						writer.close()
						delete(store.writers, key)
					}
				}
				if err := os.Remove(path); err == nil {
					removed++
				}
			}
		}
	}
	return removed, nil
}

func (store *Store) Close() {
	store.mu.Lock()
	defer store.mu.Unlock()
	for key, writer := range store.writers {
		writer.close()
		delete(store.writers, key)
	}
}

func (writer *dayWriter) close() {
	if writer.csvw != nil {
		writer.csvw.Flush()
	}
	if writer.csvBuf != nil {
		writer.csvBuf.Flush()
	}
	if writer.csvFile != nil {
		writer.csvFile.Close()
	}
	if writer.jsonBuf != nil {
		writer.jsonBuf.Flush()
	}
	if writer.jsonFile != nil {
		writer.jsonFile.Close()
	}
}

func writeJSONLine(writer *bufio.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := writer.Write(payload); err != nil {
		return err
	}
	return writer.WriteByte('\n')
}

func jsonString(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			return ""
		}
	case []any:
		if len(typed) == 0 {
			return ""
		}
	case string:
		if typed == "" {
			return ""
		}
		return typed
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(payload)
}

// scanSpans reads the newest `days` JSONL files and hands each row to visit.
func (store *Store) scanSpans(days int, visit func(SpanRow) bool) error {
	return store.scan("spans", days, func(payload []byte) bool {
		var row SpanRow
		if err := json.Unmarshal(payload, &row); err != nil {
			return true
		}
		return visit(row)
	})
}

func (store *Store) scanMetrics(days int, visit func(MetricRow) bool) error {
	return store.scan("metrics", days, func(payload []byte) bool {
		var row MetricRow
		if err := json.Unmarshal(payload, &row); err != nil {
			return true
		}
		return visit(row)
	})
}

func (store *Store) scanLogs(days int, visit func(LogRow) bool) error {
	return store.scan("logs", days, func(payload []byte) bool {
		var row LogRow
		if err := json.Unmarshal(payload, &row); err != nil {
			return true
		}
		return visit(row)
	})
}

func (store *Store) scan(signal string, days int, visit func([]byte) bool) error {
	if days <= 0 {
		days = 1
	}
	dir := filepath.Join(store.root, "jsonl", signal)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".jsonl") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	if len(names) > days {
		names = names[len(names)-days:]
	}
	// Newest first so callers that cap results still see recent telemetry.
	for index := len(names) - 1; index >= 0; index-- {
		file, err := os.Open(filepath.Join(dir, names[index]))
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if !visit([]byte(line)) {
				file.Close()
				return nil
			}
		}
		file.Close()
	}
	return nil
}

func describeStoreError(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%v", err)
}
