package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	dbxpluginsdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
)

const pluginID = "dev.yiqiui.otel"
const pluginVersion = "0.1.1"

type receiverConfig struct {
	Host          string
	Port          int
	GRPCPort      int
	RetentionDays int
	Autostart     bool
	Token         string
}

func (config receiverConfig) endpoint() string {
	return fmt.Sprintf("http://%s:%d", config.Host, config.Port)
}

type plugin struct {
	mutex       sync.Mutex
	connections map[string]receiverConfig
	store       *Store
	receiver    *Receiver
	emitter     *dbxpluginsdk.Emitter
	lastEventAt time.Time
	retention   int
}

func main() {
	root, err := dataRoot()
	if err != nil {
		log.Fatalf("otel plugin: %v", err)
	}
	store, err := NewStore(root)
	if err != nil {
		log.Fatalf("otel plugin: %v", err)
	}
	instance := &plugin{
		connections: map[string]receiverConfig{},
		store:       store,
		retention:   7,
	}
	instance.receiver = NewReceiver(store, "")
	instance.receiver.onIngest = instance.publishIngest

	metadata := dbxpluginsdk.Metadata{
		ID:           pluginID,
		Version:      pluginVersion,
		Capabilities: []string{"connections", "events"},
	}
	server := dbxpluginsdk.NewServer(metadata, instance)
	if err := server.Serve(); err != nil {
		log.Fatalf("otel plugin: %v", err)
	}
	store.Close()
}

// dataRoot prefers the host-provided persistent directory; the fallback only
// applies when the sidecar is run standalone during development.
func dataRoot() (string, error) {
	if dir := dbxpluginsdk.DataDir(); dir != "" {
		resolved := filepath.Join(dir, "otel")
		if err := os.MkdirAll(resolved, 0o700); err != nil {
			return "", err
		}
		return resolved, nil
	}
	return filepath.Abs("./otel-data")
}

func (instance *plugin) Handle(
	_ dbxpluginsdk.RequestContext,
	method string,
	params json.RawMessage,
	emitter *dbxpluginsdk.Emitter,
) (any, *dbxpluginsdk.PluginError) {
	instance.mutex.Lock()
	instance.emitter = emitter
	instance.mutex.Unlock()

	values := map[string]any{}
	if len(params) > 0 && string(params) != "null" {
		if err := json.Unmarshal(params, &values); err != nil {
			return nil, dbxpluginsdk.NewError(-32602, "Invalid request parameters")
		}
	}

	switch method {
	case "connection/test":
		return instance.testConnection(values)
	case "connection/connect":
		return instance.connect(values)
	case "connection/disconnect":
		return instance.disconnect(values)
	case "otel/status":
		return instance.receiver.Status(), nil
	case "otel/start":
		config, configError := instance.configFrom(values)
		if configError != nil {
			return nil, configError
		}
		if err := instance.receiver.StartWithGRPC(config.Host, config.Port, config.GRPCPort); err != nil {
			return nil, dbxpluginsdk.NewError(-32000, "cannot start OTLP receiver: "+err.Error())
		}
		instance.retention = config.RetentionDays
		instance.purgeNow()
		return instance.receiver.Status(), nil
	case "otel/stop":
		instance.receiver.Stop()
		return instance.receiver.Status(), nil
	case "otel/listTraces":
		return instance.listTraces(values)
	case "otel/getTrace":
		traceID, _ := values["traceId"].(string)
		if strings.TrimSpace(traceID) == "" {
			return nil, dbxpluginsdk.NewError(-32602, "traceId is required")
		}
		spans, err := instance.store.GetTrace(strings.TrimSpace(traceID))
		if err != nil {
			return nil, dbxpluginsdk.NewError(-32000, err.Error())
		}
		return map[string]any{"traceId": traceID, "spans": spans, "spanCount": len(spans)}, nil
	case "otel/listMetrics":
		limit := intValue(values["limit"], 100)
		name, _ := values["name"].(string)
		rows, err := instance.store.ListMetrics(limit, name)
		if err != nil {
			return nil, dbxpluginsdk.NewError(-32000, err.Error())
		}
		return map[string]any{"series": rows, "count": len(rows)}, nil
	case "otel/listLogs":
		limit := intValue(values["limit"], 100)
		level, _ := values["level"].(string)
		search, _ := values["search"].(string)
		rows, err := instance.store.ListLogs(limit, level, search)
		if err != nil {
			return nil, dbxpluginsdk.NewError(-32000, err.Error())
		}
		return map[string]any{"logs": rows, "count": len(rows)}, nil
	case "otel/analysisSql":
		return instance.store.AnalysisSQL(), nil
	case "otel/sample":
		count := instance.store.IngestSample()
		return map[string]any{"ingestedSpans": count, "status": instance.receiver.Status()}, nil
	case "otel/purge":
		days := intValue(values["days"], instance.retention)
		removed, err := instance.store.Purge(days)
		if err != nil {
			return nil, dbxpluginsdk.NewError(-32000, err.Error())
		}
		return map[string]any{"removedFiles": removed, "retentionDays": days}, nil
	default:
		return nil, dbxpluginsdk.MethodNotFound(method)
	}
}

func (instance *plugin) testConnection(values map[string]any) (any, *dbxpluginsdk.PluginError) {
	config, configError := instance.configFrom(values)
	if configError != nil {
		return nil, configError
	}
	// Probe without disturbing a running receiver: bind, read the port, release.
	probe, err := probeListen(config.Host, config.Port)
	if err != nil {
		return nil, dbxpluginsdk.NewError(-32000, "endpoint unavailable: "+err.Error())
	}
	defer probe.Close()
	return map[string]any{
		"success": true,
		"message": fmt.Sprintf("OTLP/HTTP 将监听 %s/v1/traces（protobuf 与 JSON 均可）", config.endpoint()) +
			grpcProbeNote(config),
		"endpoint": config.endpoint(),
	}, nil
}

// grpcProbeNote reports whether the optional gRPC port is also usable, without
// failing the test when only gRPC is blocked.
func grpcProbeNote(config receiverConfig) string {
	if config.GRPCPort == 0 {
		return "；gRPC 已禁用"
	}
	probe, err := probeListen(config.Host, config.GRPCPort)
	if err != nil {
		return fmt.Sprintf("；gRPC 端口 %d 不可用：%v", config.GRPCPort, err)
	}
	actual := probe.Addr().String()
	probe.Close()
	if strings.HasSuffix(actual, fmt.Sprintf(":%d", config.GRPCPort)) {
		return fmt.Sprintf("；gRPC 将监听 %s:%d", config.Host, config.GRPCPort)
	}
	return fmt.Sprintf("；gRPC 端口 %d 被占用，将改用随机端口", config.GRPCPort)
}

func (instance *plugin) connect(values map[string]any) (any, *dbxpluginsdk.PluginError) {
	connectionID, idError := connectionID(values)
	if idError != nil {
		return nil, idError
	}
	config, configError := instance.configFrom(values)
	if configError != nil {
		return nil, configError
	}
	instance.mutex.Lock()
	already := len(instance.connections) > 0
	instance.mutex.Unlock()
	if !already {
		if err := instance.receiver.StartWithGRPC(config.Host, config.Port, config.GRPCPort); err != nil {
			return nil, dbxpluginsdk.NewError(-32000, "cannot start OTLP receiver: "+err.Error())
		}
		instance.retention = config.RetentionDays
		instance.purgeNow()
	}
	instance.mutex.Lock()
	instance.connections[connectionID] = config
	instance.mutex.Unlock()
	status := instance.receiver.Status()
	status["connectionId"] = connectionID
	return status, nil
}

func (instance *plugin) disconnect(values map[string]any) (any, *dbxpluginsdk.PluginError) {
	connectionID, idError := connectionID(values)
	if idError != nil {
		return nil, idError
	}
	instance.mutex.Lock()
	delete(instance.connections, connectionID)
	remaining := len(instance.connections)
	instance.mutex.Unlock()
	if remaining == 0 {
		instance.receiver.Stop()
	}
	status := instance.receiver.Status()
	status["connectionId"] = connectionID
	return status, nil
}

func (instance *plugin) listTraces(values map[string]any) (any, *dbxpluginsdk.PluginError) {
	filter := traceFilter{
		Limit:   intValue(values["limit"], 50),
		Service: stringOr(values["service"]),
		Errors:  boolOr(values["errors"]),
		Search:  stringOr(values["search"]),
		ScanDay: intValue(values["days"], 3),
	}
	if minMs := intValue(values["minDurationMs"], 0); minMs > 0 {
		filter.MinNs = int64(minMs) * int64(time.Millisecond)
	}
	traces, err := instance.store.ListTraces(filter)
	if err != nil {
		return nil, dbxpluginsdk.NewError(-32000, err.Error())
	}
	return map[string]any{"traces": traces, "count": len(traces)}, nil
}

// configFrom reads provider-declared fields. Config-bound values arrive under
// external_config; secrets are hydrated at the top level by the host.
func (instance *plugin) configFrom(values map[string]any) (receiverConfig, *dbxpluginsdk.PluginError) {
	connection, _ := values["connection"].(map[string]any)
	external, _ := connection["external_config"].(map[string]any)
	if external == nil {
		external, _ = values["external_config"].(map[string]any)
	}
	pick := func(key string) any {
		if external != nil {
			if value, ok := external[key]; ok && value != nil && value != "" {
				return value
			}
		}
		if connection != nil {
			if value, ok := connection[key]; ok && value != nil && value != "" {
				return value
			}
		}
		return values[key]
	}
	config := receiverConfig{Host: "127.0.0.1", Port: 4318, GRPCPort: 4317, RetentionDays: 7, Autostart: true}
	if host := strings.TrimSpace(stringOr(pick("listen_host"))); host != "" {
		config.Host = host
	}
	if port := intValue(pick("listen_port"), 0); port > 0 {
		config.Port = port
	}
	// grpc_port 0 disables the gRPC surface, which keeps the footprint to one socket.
	if raw := pick("grpc_port"); raw != nil {
		if grpcPort := intValue(raw, -1); grpcPort >= 0 {
			config.GRPCPort = grpcPort
		}
	}
	if days := intValue(pick("retention_days"), 0); days > 0 {
		config.RetentionDays = days
	}
	if flag, ok := pick("autostart").(bool); ok {
		config.Autostart = flag
	}
	if strings.EqualFold(stringOr(pick("autostart")), "false") {
		config.Autostart = false
	}
	if token := strings.TrimSpace(stringOr(pick("ingest_token"))); token != "" {
		config.Token = token
		instance.receiver.setToken(token)
	}
	if config.Port < 1 || config.Port > 65535 {
		return config, dbxpluginsdk.NewError(-32602, "listen_port must be between 1 and 65535")
	}
	if config.GRPCPort < 0 || config.GRPCPort > 65535 {
		return config, dbxpluginsdk.NewError(-32602, "grpc_port must be between 0 and 65535")
	}
	if config.Token != "" {
		instance.receiver.mu.Lock()
		instance.receiver.token = config.Token
		instance.receiver.mu.Unlock()
	}
	return config, nil
}

func (instance *plugin) purgeNow() {
	if instance.retention <= 0 {
		return
	}
	if _, err := instance.store.Purge(instance.retention); err != nil {
		log.Printf("otel plugin: retention purge failed: %v", err)
	}
}

// publishIngest forwards a throttled live-tail event to the workbench. The host
// only relays events when the plugin declares host.events.
func (instance *plugin) publishIngest(signal string, count int, endpoint string) {
	instance.mutex.Lock()
	emitter := instance.emitter
	if time.Since(instance.lastEventAt) < 700*time.Millisecond {
		instance.mutex.Unlock()
		return
	}
	instance.lastEventAt = time.Now()
	instance.mutex.Unlock()
	if emitter == nil {
		return
	}
	_ = emitter.Event("otel/ingested", map[string]any{
		"signal":   signal,
		"count":    count,
		"endpoint": endpoint,
		"at":       time.Now().UTC().Format(time.RFC3339),
		"counts":   instance.store.Counts(),
	})
}

func connectionID(values map[string]any) (string, *dbxpluginsdk.PluginError) {
	connection, _ := values["connection"].(map[string]any)
	id, _ := connection["id"].(string)
	if id == "" {
		if direct, ok := values["connectionId"].(string); ok && direct != "" {
			return direct, nil
		}
		return "", dbxpluginsdk.NewError(-32602, "Missing connection id")
	}
	return id, nil
}

func stringOr(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case nil:
		return ""
	default:
		return fmt.Sprint(typed)
	}
}

func boolOr(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, err := strconv.ParseBool(typed)
		return err == nil && parsed
	default:
		return false
	}
}

func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case int64:
		return int(typed)
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return fallback
		}
		return parsed
	default:
		return fallback
	}
}
