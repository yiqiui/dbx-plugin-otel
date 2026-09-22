package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type traceFilter struct {
	Limit   int
	Service string
	Since   int64 // unix nano; 0 = no lower bound
	Errors  bool
	MinNs   int64
	Search  string
	ScanDay int
}

func (store *Store) ListTraces(filter traceFilter) ([]map[string]any, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.ScanDay <= 0 {
		filter.ScanDay = 3
	}
	grouped := map[string][]SpanRow{}
	var scanErr error
	scanErr = store.scanSpans(filter.ScanDay, func(row SpanRow) bool {
		if row.TraceID == "" {
			return true
		}
		grouped[row.TraceID] = append(grouped[row.TraceID], row)
		return true
	})
	if scanErr != nil {
		return nil, scanErr
	}
	summaries := summarize(grouped)
	kept := make([]traceSummary, 0, len(summaries))
	for _, summary := range summaries {
		if filter.Service != "" && !matchesService(summary.Services, filter.Service) {
			continue
		}
		if filter.Errors && summary.ErrorCount == 0 {
			continue
		}
		if filter.Since > 0 && summary.EndNano < filter.Since {
			continue
		}
		if filter.MinNs > 0 && summary.DurationNs < filter.MinNs {
			continue
		}
		if filter.Search != "" && !strings.Contains(strings.ToLower(summary.RootName+" "+summary.TraceID), strings.ToLower(filter.Search)) {
			continue
		}
		kept = append(kept, summary)
	}
	sort.Slice(kept, func(i, j int) bool {
		if kept[i].ReceivedAt != kept[j].ReceivedAt {
			return kept[i].ReceivedAt > kept[j].ReceivedAt
		}
		return kept[i].StartNano > kept[j].StartNano
	})
	if len(kept) > filter.Limit {
		kept = kept[:filter.Limit]
	}
	out := make([]map[string]any, 0, len(kept))
	for _, summary := range kept {
		out = append(out, map[string]any{
			"trace_id":             summary.TraceID,
			"root_name":            summary.RootName,
			"service":              summary.Service,
			"services":             summary.Services,
			"span_count":           summary.SpanCount,
			"error_count":          summary.ErrorCount,
			"start_unix_nano":      summary.StartNano,
			"end_unix_nano":        summary.EndNano,
			"duration_ns":          summary.DurationNs,
			"duration_readable":    formatDuration(summary.DurationNs),
			"max_span_duration_ns": summary.MaxDuration,
			"slowest_span":         summary.SlowestSpan,
			"started_at":           nanoToRFC3339(summary.StartNano),
		})
	}
	return out, nil
}

func (store *Store) GetTrace(traceID string) ([]map[string]any, error) {
	var spans []SpanRow
	if err := store.scanSpans(7, func(row SpanRow) bool {
		if row.TraceID == traceID {
			spans = append(spans, row)
		}
		return true
	}); err != nil {
		return nil, err
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].StartUnixNano < spans[j].StartUnixNano })
	out := make([]map[string]any, 0, len(spans))
	for _, row := range spans {
		out = append(out, map[string]any{
			"trace_id":             row.TraceID,
			"span_id":              row.SpanID,
			"parent_span_id":       row.ParentSpanID,
			"name":                 row.Name,
			"kind":                 row.Kind,
			"service":              row.Service,
			"start_unix_nano":      row.StartUnixNano,
			"end_unix_nano":        row.EndUnixNano,
			"duration_ns":          row.DurationNs,
			"duration_readable":    formatDuration(row.DurationNs),
			"status_code":          row.StatusCode,
			"status_message":       row.StatusMessage,
			"attributes":           row.Attributes,
			"resource_attributes":  row.ResourceAttributes,
			"events":               row.Events,
			"links":                row.Links,
		})
	}
	return out, nil
}

func (store *Store) ListMetrics(limit int, name string) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	type series struct {
		Name       string
		Kind       string
		Unit       string
		Service    string
		Attributes string
		Latest     MetricRow
		Samples    int64
	}
	index := map[string]*series{}
	var order []string
	if err := store.scanMetrics(3, func(row MetricRow) bool {
		if name != "" && !strings.Contains(strings.ToLower(row.MetricName), strings.ToLower(name)) {
			return true
		}
		key := row.MetricName + "|" + row.Kind + "|" + row.Service + "|" + jsonString(row.Attributes)
		entry, ok := index[key]
		if !ok {
			entry = &series{Name: row.MetricName, Kind: row.Kind, Unit: row.Unit, Service: row.Service, Attributes: jsonString(row.Attributes)}
			index[key] = entry
			order = append(order, key)
		}
		entry.Samples++
		if row.TimeUnixNano >= entry.Latest.TimeUnixNano {
			entry.Latest = row
		}
		return true
	}); err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(order)))
	out := make([]map[string]any, 0, len(order))
	for _, key := range order {
		entry := index[key]
		out = append(out, map[string]any{
			"metric_name":   entry.Name,
			"kind":          entry.Kind,
			"unit":          entry.Unit,
			"service":       entry.Service,
			"attributes":    entry.Attributes,
			"samples":       entry.Samples,
			"latest_value":  entry.Latest.ValueDouble,
			"latest_time":   nanoToRFC3339(entry.Latest.TimeUnixNano),
			"latest_json":   entry.Latest.ValueJSON,
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (store *Store) ListLogs(limit int, level string, search string) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	out := make([]map[string]any, 0, limit)
	if err := store.scanLogs(3, func(row LogRow) bool {
		if level != "" && !strings.EqualFold(row.SeverityText, level) {
			return true
		}
		if search != "" && !strings.Contains(strings.ToLower(row.Body), strings.ToLower(search)) {
			return true
		}
		out = append(out, map[string]any{
			"timestamp":       nanoToRFC3339(row.TimestampUnixNano),
			"severity_text":   row.SeverityText,
			"severity_number": row.SeverityNumber,
			"service":         row.Service,
			"body":            row.Body,
			"attributes":      row.Attributes,
			"trace_id":        row.TraceID,
			"span_id":         row.SpanID,
		})
		return len(out) < limit
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// AnalysisSQL hands the user ready-to-run DuckDB statements with the real CSV
// paths already filled in, which is the whole point of landing columnar files:
// heavy aggregation belongs in SQL, not in the plugin's bounded UI snapshot.
func (store *Store) AnalysisSQL() map[string]any {
	spans := store.CSVGlob("spans")
	metrics := store.CSVGlob("metrics")
	logs := store.CSVGlob("logs")
	files, _ := store.FilesOnDisk()
	return map[string]any{
		"csvGlob": map[string]string{"spans": spans, "metrics": metrics, "logs": logs},
		"files":   files,
		"howTo":   "在 DBX 里新建一个 DuckDB 连接（文件型），直接执行下面的语句；无需任何服务端。",
		"queries": []map[string]string{
			{
				"title": "最慢的 20 条 trace",
				"sql": fmt.Sprintf(
					"SELECT trace_id, arg_min(name, start_unix_nano) AS root_name, COUNT(*) AS spans, MAX(end_unix_nano) - MIN(start_unix_nano) AS duration_ns FROM read_csv_auto('%s') GROUP BY trace_id ORDER BY duration_ns DESC LIMIT 20", spans),
			},
			{
				"title": "按服务统计 span 数量与 P95 耗时",
				"sql": fmt.Sprintf(
					"SELECT service, COUNT(*) AS spans, quantile_cont(duration_ns / 1e6, 0.95) AS p95_ms FROM read_csv_auto('%s') GROUP BY service ORDER BY spans DESC", spans),
			},
			{
				"title": "错误率（按根 span 名称）",
				"sql": fmt.Sprintf(
					"SELECT name, COUNT(*) FILTER (WHERE status_code = 'error') AS errors, COUNT(*) AS total, ROUND(100.0 * errors / total, 2) AS error_pct FROM read_csv_auto('%s') GROUP BY name HAVING total > 5 ORDER BY error_pct DESC LIMIT 20", spans),
			},
			{
				"title": "从 attributes 里取 HTTP 路由",
				"sql": fmt.Sprintf(
					"SELECT trace_id, name, try_cast(json_extract(attributes_json, '$.\"http.route\"') AS VARCHAR) AS route FROM read_csv_auto('%s') WHERE attributes_json IS NOT NULL AND route IS NOT NULL LIMIT 50", spans),
			},
			{
				"title": "指标趋势（每分钟均值）",
				"sql": fmt.Sprintf(
					"SELECT date_trunc('minute', make_timestamp(time_unix_nano / 1000)) AS minute, metric_name, AVG(value_double) AS avg_value FROM read_csv_auto('%s') GROUP BY minute, metric_name ORDER BY minute DESC LIMIT 100", metrics),
			},
			{
				"title": "错误日志关联 trace",
				"sql": fmt.Sprintf(
					"SELECT timestamp_unix_nano, service, body, trace_id FROM read_csv_auto('%s') WHERE severity_number >= 17 AND trace_id <> '' ORDER BY timestamp_unix_nano DESC LIMIT 50", logs),
			},
		},
	}
}

// IngestSample writes a small synthetic trace so the plugin is demoable before
// any application is pointed at it.
func (store *Store) IngestSample() int {
	base := time.Now().Add(-3 * time.Second).UnixNano()
	service := "demo-checkout"
	resource := map[string]any{"service.name": service, "deployment.environment": "local"}
	rows := []SpanRow{
		{
			TraceID: "d1000000000000000000000000000001", SpanID: "1000000000000001", ParentSpanID: "",
			Name: "POST /checkout", Kind: "server", Service: service,
			StartUnixNano: base, EndUnixNano: base + int64(820*time.Millisecond),
			StatusCode: "unset", Attributes: map[string]any{"http.method": "POST", "http.route": "/checkout", "http.status_code": int64(200)},
			ResourceAttributes: resource,
		},
		{
			TraceID: "d1000000000000000000000000000001", SpanID: "1000000000000002", ParentSpanID: "1000000000000001",
			Name: "cart.validate", Kind: "internal", Service: service,
			StartUnixNano: base + int64(20*time.Millisecond), EndUnixNano: base + int64(95*time.Millisecond),
			StatusCode: "unset", Attributes: map[string]any{"cart.items": int64(3)}, ResourceAttributes: resource,
		},
		{
			TraceID: "d1000000000000000000000000000001", SpanID: "1000000000000003", ParentSpanID: "1000000000000001",
			Name: "SELECT payments", Kind: "client", Service: service,
			StartUnixNano: base + int64(120*time.Millisecond), EndUnixNano: base + int64(640*time.Millisecond),
			StatusCode: "error", StatusMessage: "deadlock detected",
			Attributes:         map[string]any{"db.system": "postgresql", "db.statement": "SELECT * FROM payments WHERE id = $1"},
			ResourceAttributes: resource,
		},
		{
			TraceID: "d1000000000000000000000000000001", SpanID: "1000000000000004", ParentSpanID: "1000000000000001",
			Name: "kafka.publish order.created", Kind: "producer", Service: service,
			StartUnixNano: base + int64(660*time.Millisecond), EndUnixNano: base + int64(700*time.Millisecond),
			StatusCode:         "unset",
			Attributes:         map[string]any{"messaging.system": "kafka", "messaging.destination.name": "order.created"},
			ResourceAttributes: resource,
		},
	}
	for index := range rows {
		rows[index].ReceivedAtUnixNano = time.Now().UnixNano()
	}
	logs := []LogRow{
		{
			TimestampUnixNano: base + int64(640*time.Millisecond), SeverityText: "ERROR", SeverityNumber: 17,
			Service: service, Body: "payment query failed: deadlock detected",
			Attributes: map[string]any{"exception.type": "org.postgresql.util.PSQLException"},
			TraceID:    "d1000000000000000000000000000001", SpanID: "1000000000000003",
			ResourceAttributes: resource,
		},
	}
	metrics := []MetricRow{
		{
			MetricName: "checkout.duration", Unit: "ms", Kind: "histogram", Service: service,
			Attributes: map[string]any{"http.route": "/checkout"}, ResourceAttributes: resource,
			TimeUnixNano: base + int64(820*time.Millisecond), ValueDouble: 820,
			ValueJSON: map[string]any{"count": int64(1), "sum": float64(820)},
		},
	}
	if err := store.AppendSpans(rows); err != nil {
		return 0
	}
	_ = store.AppendLogs(logs)
	_ = store.AppendMetrics(metrics)
	return len(rows)
}

func matchesService(services []string, want string) bool {
	for _, service := range services {
		if strings.EqualFold(service, want) {
			return true
		}
	}
	return false
}

func nanoToRFC3339(nanos int64) string {
	if nanos <= 0 {
		return ""
	}
	return time.Unix(0, nanos).Local().Format(time.RFC3339Nano)
}
