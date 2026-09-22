package main

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func spanKindName(kind tracepb.Span_SpanKind) string {
	switch kind {
	case tracepb.Span_SPAN_KIND_SERVER:
		return "server"
	case tracepb.Span_SPAN_KIND_CLIENT:
		return "client"
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return "producer"
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return "consumer"
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return "internal"
	default:
		return "unspecified"
	}
}

func statusCodeName(status *tracepb.Status) string {
	if status == nil {
		return "unset"
	}
	switch status.Code {
	case tracepb.Status_STATUS_CODE_OK:
		return "ok"
	case tracepb.Status_STATUS_CODE_ERROR:
		return "error"
	default:
		return "unset"
	}
}

func hexID(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	return hex.EncodeToString(value)
}

func attributesToMap(list *commonpb.KeyValueList) map[string]any {
	if list == nil || len(list.Values) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(list.Values))
	for _, item := range list.Values {
		if item == nil || item.Key == "" {
			continue
		}
		out[item.Key] = anyValue(item.Value)
	}
	return out
}

func anyValue(value *commonpb.AnyValue) any {
	if value == nil {
		return nil
	}
	switch typed := value.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return typed.StringValue
	case *commonpb.AnyValue_BoolValue:
		return typed.BoolValue
	case *commonpb.AnyValue_IntValue:
		return typed.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return typed.DoubleValue
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(typed.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		if typed.ArrayValue == nil {
			return nil
		}
		items := make([]any, 0, len(typed.ArrayValue.Values))
		for _, item := range typed.ArrayValue.Values {
			items = append(items, anyValue(item))
		}
		return items
	case *commonpb.AnyValue_KvlistValue:
		if typed.KvlistValue == nil {
			return nil
		}
		return attributesToMap(&commonpb.KeyValueList{Values: typed.KvlistValue.Values})
	}
	return nil
}

func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, item := range resource.Attributes {
		if item != nil && item.Key == "service.name" {
			if name, ok := anyValue(item.Value).(string); ok {
				return name
			}
		}
	}
	return ""
}

func flattenSpans(items []*tracepb.ResourceSpans) []SpanRow {
	var rows []SpanRow
	for _, resourceSpans := range items {
		if resourceSpans == nil {
			continue
		}
		service := serviceName(resourceSpans.Resource)
		resourceJSON := map[string]any{}
		if resourceSpans.Resource != nil {
			resourceJSON = attributesToMap(&commonpb.KeyValueList{Values: resourceSpans.Resource.Attributes})
		}
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			for _, span := range scopeSpans.Spans {
				if span == nil {
					continue
				}
				start := int64(span.StartTimeUnixNano)
				end := int64(span.EndTimeUnixNano)
				duration := end - start
				if duration < 0 {
					duration = 0
				}
				events := make([]any, 0, len(span.Events))
				for _, event := range span.Events {
					if event == nil {
						continue
					}
					events = append(events, map[string]any{
						"name":            event.Name,
						"time_unix_nano":  int64(event.TimeUnixNano),
						"attributes":      attributesToMap(&commonpb.KeyValueList{Values: event.Attributes}),
						"dropped_count":   int32(event.DroppedAttributesCount),
					})
				}
				links := make([]any, 0, len(span.Links))
				for _, link := range span.Links {
					if link == nil {
						continue
					}
					links = append(links, map[string]any{
						"trace_id":       hexID(link.TraceId),
						"span_id":        hexID(link.SpanId),
						"trace_state":    link.TraceState,
						"attributes":     attributesToMap(&commonpb.KeyValueList{Values: link.Attributes}),
						"dropped_count":  int32(link.DroppedAttributesCount),
					})
				}
				rows = append(rows, SpanRow{
					TraceID:            hexID(span.TraceId),
					SpanID:             hexID(span.SpanId),
					ParentSpanID:       hexID(span.ParentSpanId),
					Name:               span.Name,
					Kind:               spanKindName(span.Kind),
					Service:            service,
					StartUnixNano:      start,
					EndUnixNano:        end,
					DurationNs:         duration,
					StatusCode:         statusCodeName(span.Status),
					StatusMessage:      statusMessage(span.Status),
					Attributes:         attributesToMap(&commonpb.KeyValueList{Values: span.Attributes}),
					ResourceAttributes: resourceJSON,
					Events:             events,
					Links:              links,
				})
			}
		}
	}
	return rows
}

func statusMessage(status *tracepb.Status) string {
	if status == nil {
		return ""
	}
	return status.Message
}

func flattenMetrics(items []*metricpb.ResourceMetrics) []MetricRow {
	var rows []MetricRow
	for _, resourceMetrics := range items {
		if resourceMetrics == nil {
			continue
		}
		service := serviceName(resourceMetrics.Resource)
		resourceJSON := map[string]any{}
		if resourceMetrics.Resource != nil {
			resourceJSON = attributesToMap(&commonpb.KeyValueList{Values: resourceMetrics.Resource.Attributes})
		}
		for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
			for _, metric := range scopeMetrics.Metrics {
				if metric == nil {
					continue
				}
				base := MetricRow{
					MetricName:         metric.Name,
					Description:        metric.Description,
					Unit:               metric.Unit,
					Service:            service,
					ResourceAttributes: resourceJSON,
				}
				switch data := metric.Data.(type) {
				case *metricpb.Metric_Gauge:
					base.Kind = "gauge"
					for _, point := range data.Gauge.DataPoints {
						row := base
						fillNumberPoint(&row, point)
						rows = append(rows, row)
					}
				case *metricpb.Metric_Sum:
					base.Kind = "sum"
					if data.Sum != nil && data.Sum.IsMonotonic {
						base.Kind = "sum.monotonic"
					}
					for _, point := range data.Sum.DataPoints {
						row := base
						fillNumberPoint(&row, point)
						rows = append(rows, row)
					}
				case *metricpb.Metric_Histogram:
					base.Kind = "histogram"
					for _, point := range data.Histogram.DataPoints {
						row := base
						if point == nil {
							continue
						}
						row.Attributes = attributesToMap(&commonpb.KeyValueList{Values: point.Attributes})
						row.StartUnixNano = int64(point.StartTimeUnixNano)
						row.TimeUnixNano = int64(point.TimeUnixNano)
						row.ValueDouble = point.GetSum()
						row.ValueJSON = map[string]any{
							"count":           int64(point.Count),
							"sum":             point.GetSum(),
							"bucket_counts":   point.BucketCounts,
							"explicit_bounds": point.ExplicitBounds,
							"min":             point.GetMin(),
							"max":             point.GetMax(),
						}
						rows = append(rows, row)
					}
				case *metricpb.Metric_ExponentialHistogram:
					base.Kind = "exponential_histogram"
					for _, point := range data.ExponentialHistogram.DataPoints {
						row := base
						if point == nil {
							continue
						}
						row.Attributes = attributesToMap(&commonpb.KeyValueList{Values: point.Attributes})
						row.TimeUnixNano = int64(point.TimeUnixNano)
						row.ValueJSON = map[string]any{"count": int64(point.Count), "scale": int32(point.Scale)}
						rows = append(rows, row)
					}
				case *metricpb.Metric_Summary:
					base.Kind = "summary"
					for _, point := range data.Summary.DataPoints {
						row := base
						if point == nil {
							continue
						}
						row.Attributes = attributesToMap(&commonpb.KeyValueList{Values: point.Attributes})
						row.TimeUnixNano = int64(point.TimeUnixNano)
						row.ValueDouble = float64(point.Sum)
						row.ValueJSON = map[string]any{"count": int64(point.Count), "quantiles": point.QuantileValues}
						rows = append(rows, row)
					}
				}
			}
		}
	}
	return rows
}

func fillNumberPoint(row *MetricRow, point *metricpb.NumberDataPoint) {
	if point == nil {
		return
	}
	row.Attributes = attributesToMap(&commonpb.KeyValueList{Values: point.Attributes})
	row.StartUnixNano = int64(point.StartTimeUnixNano)
	row.TimeUnixNano = int64(point.TimeUnixNano)
	switch value := point.Value.(type) {
	case *metricpb.NumberDataPoint_AsDouble:
		row.ValueDouble = value.AsDouble
	case *metricpb.NumberDataPoint_AsInt:
		row.ValueDouble = float64(value.AsInt)
		row.ValueJSON = map[string]any{"as_int": value.AsInt}
	}
}

func flattenLogs(items []*logspb.ResourceLogs) []LogRow {
	var rows []LogRow
	for _, resourceLogs := range items {
		if resourceLogs == nil {
			continue
		}
		service := serviceName(resourceLogs.Resource)
		resourceJSON := map[string]any{}
		if resourceLogs.Resource != nil {
			resourceJSON = attributesToMap(&commonpb.KeyValueList{Values: resourceLogs.Resource.Attributes})
		}
		for _, scopeLogs := range resourceLogs.ScopeLogs {
			for _, record := range scopeLogs.LogRecords {
				if record == nil {
					continue
				}
				rows = append(rows, LogRow{
					TimestampUnixNano:  int64(record.TimeUnixNano),
					SeverityText:       record.SeverityText,
					SeverityNumber:     int32(record.SeverityNumber),
					Service:            service,
					Body:               bodyText(record.Body),
					Attributes:         attributesToMap(&commonpb.KeyValueList{Values: record.Attributes}),
					ResourceAttributes: resourceJSON,
					TraceID:            hexID(record.TraceId),
					SpanID:             hexID(record.SpanId),
				})
			}
		}
	}
	return rows
}

func bodyText(value *commonpb.AnyValue) string {
	parsed := anyValue(value)
	switch typed := parsed.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		payload, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return string(payload)
	}
}

// traceSummary aggregates spans belonging to one trace id.
type traceSummary struct {
	TraceID     string   `json:"trace_id"`
	RootName    string   `json:"root_name"`
	Service     string   `json:"service"`
	Services    []string `json:"services"`
	SpanCount   int      `json:"span_count"`
	ErrorCount  int      `json:"error_count"`
	StartNano   int64    `json:"start_unix_nano"`
	EndNano     int64    `json:"end_unix_nano"`
	DurationNs  int64    `json:"duration_ns"`
	ReceivedAt  int64    `json:"received_at_unix_nano"`
	MaxDuration int64    `json:"max_span_duration_ns"`
	SlowestSpan string   `json:"slowest_span"`
}

func summarize(spans map[string][]SpanRow) []traceSummary {
	out := make([]traceSummary, 0, len(spans))
	for traceID, rows := range spans {
		summary := traceSummary{TraceID: traceID, SpanCount: len(rows)}
		services := map[string]struct{}{}
		known := map[string]struct{}{}
		for _, row := range rows {
			known[row.SpanID] = struct{}{}
		}
		for _, row := range rows {
			if row.Service != "" {
				services[row.Service] = struct{}{}
			}
			if row.StartUnixNano == 0 || row.StartUnixNano < summary.StartNano || summary.StartNano == 0 {
				summary.StartNano = row.StartUnixNano
			}
			if row.EndUnixNano > summary.EndNano {
				summary.EndNano = row.EndUnixNano
			}
			if strings.EqualFold(row.StatusCode, "error") {
				summary.ErrorCount++
			}
			if row.DurationNs > summary.MaxDuration {
				summary.MaxDuration = row.DurationNs
				summary.SlowestSpan = row.Name
			}
			if row.ReceivedAtUnixNano > summary.ReceivedAt {
				summary.ReceivedAt = row.ReceivedAtUnixNano
			}
		}
		// The root is the span that references no parent, or whose parent is not
		// part of this trace (a cross-process entry point). Ties break on the
		// earliest start so a partially exported trace still reports a usable root.
		var root *SpanRow
		for index := range rows {
			row := &rows[index]
			if row.ParentSpanID != "" {
				if _, present := known[row.ParentSpanID]; present {
					continue
				}
			}
			if root == nil || row.StartUnixNano < root.StartUnixNano {
				root = row
			}
		}
		if root == nil && len(rows) > 0 {
			root = &rows[0]
		}
		if root != nil {
			summary.RootName = root.Name
			summary.Service = root.Service
		}
		for name := range services {
			summary.Services = append(summary.Services, name)
		}
		if summary.EndNano > summary.StartNano {
			summary.DurationNs = summary.EndNano - summary.StartNano
		}
		out = append(out, summary)
	}
	return out
}

func formatDuration(nanos int64) string {
	switch {
	case nanos >= int64(time.Second)*1:
		return strconv.FormatFloat(float64(nanos)/float64(time.Second), 'f', 3, 64) + "s"
	case nanos >= int64(time.Millisecond):
		return strconv.FormatFloat(float64(nanos)/float64(time.Millisecond), 'f', 2, 64) + "ms"
	case nanos >= int64(time.Microsecond):
		return strconv.FormatFloat(float64(nanos)/float64(time.Microsecond), 'f', 2, 64) + "µs"
	default:
		return strconv.FormatInt(nanos, 10) + "ns"
	}
}
