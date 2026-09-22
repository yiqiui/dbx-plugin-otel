// End-to-end: real sidecar + real OTel Go SDK application, both transports.
//   node dev/e2e.mjs
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(\w:)/, "$1")), "..");
const exe = path.join(root, "build", process.platform === "win32" ? "dbx-plugin-otel.exe" : "dbx-plugin-otel");
const app = path.join(root, "examples", "checkout-service");
const dataDir = mkdtempSync(path.join(tmpdir(), "dbx-otel-e2e-"));
process.env.DBX_PLUGIN_DATA_DIR = dataDir;

const child = spawn(exe, [], { stdio: ["pipe", "pipe", "inherit"], env: process.env });
let buffer = "";
const pending = new Map();
child.stdout.on("data", (chunk) => {
  buffer += chunk.toString("utf8");
  let index;
  while ((index = buffer.indexOf("\n")) >= 0) {
    const line = buffer.slice(0, index).trim();
    buffer = buffer.slice(index + 1);
    if (!line) continue;
    const message = JSON.parse(line);
    if (message.id !== undefined && pending.has(message.id)) {
      const { resolve, reject } = pending.get(message.id);
      pending.delete(message.id);
      message.error ? reject(new Error(JSON.stringify(message.error))) : resolve(message.result);
    }
  }
});
let id = 1;
const call = (method, params, ms = 15000) => new Promise((resolve, reject) => {
  const rid = id++;
  const timer = setTimeout(() => { pending.delete(rid); reject(new Error(method + " timed out")); }, ms);
  pending.set(rid, { resolve: (value) => { clearTimeout(timer); resolve(value); }, reject: (error) => { clearTimeout(timer); reject(error); } });
  child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id: rid, method, params }) + "\n");
});

const results = [];
const check = (name, ok, detail) => {
  results.push({ name, ok });
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${ok ? "" : "  -> " + detail}`);
};

try {
  await call("plugin/initialize", { host: { protocolVersions: [1] }, plugin: { id: "dev.yiqiui.otel", version: "0.1.0" }, permissions: [] });
  const connected = await call("connection/connect", {
    provider: { id: "dev.yiqiui.otel.receiver", databaseType: "otel" },
    connection: { id: "e2e", db_type: "plugin", external_config: { listen_host: "127.0.0.1", listen_port: 4318, grpc_port: 4317, retention_days: 7 } },
    runtime: { host: "127.0.0.1", port: 0 },
  });
  check("receiver listening (HTTP)", connected.listening === true, JSON.stringify(connected));
  check("receiver listening (gRPC)", connected.grpcListening === true, JSON.stringify(connected));
  console.log(`      HTTP ${connected.endpoint}   gRPC ${connected.grpcEndpoint}`);

  const httpEndpoint = connected.endpoint;
  const grpcEndpoint = String(connected.grpcEndpoint).replace("grpc://", "");

  const runApp = (env, label) => {
    // GOOS/GOARCH/CGO_ENABLED must be pinned per invocation: `go env` may pin a
    // cross-compile target globally, which breaks `go run` on this host.
    const result = spawnSync("go", ["run", "."], {
      cwd: app,
      env: {
        ...process.env,
        GOOS: process.platform === "win32" ? "windows" : process.platform === "darwin" ? "darwin" : "linux",
        GOARCH: process.arch === "arm64" ? "arm64" : "amd64",
        CGO_ENABLED: "0",
        ...env,
      },
      encoding: "utf8",
      timeout: 240000,
    });
    const output = `${result.stdout || ""}${result.stderr || ""}`;
    check(`${label} exported successfully`, result.status === 0, output.slice(0, 400));
    return output;
  };

  runApp({ OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf", OTEL_EXPORTER_OTLP_ENDPOINT: httpEndpoint }, "http/protobuf app");
  runApp({ OTEL_EXPORTER_OTLP_PROTOCOL: "grpc", OTEL_EXPORTER_OTLP_ENDPOINT: grpcEndpoint }, "grpc app");

  const listed = await call("otel/listTraces", { limit: 50, service: "checkout-service", days: 3 });
  const traces = listed.traces || [];
  check("10 traces from the demo service", traces.length === 10, `got ${traces.length}`);
  check("each trace has 4 spans", traces.length === 10 && traces.every((row) => row.span_count === 4), JSON.stringify(traces.map((row) => row.span_count)));
  check("two errored traces captured", traces.length === 10 && traces.filter((row) => row.error_count > 0).length === 2,
    JSON.stringify(traces.map((row) => row.error_count)));
  check("root span named", traces.length === 10 && traces.every((row) => row.root_name === "POST /checkout"), JSON.stringify(traces.map((row) => row.root_name)));
  check("both transports contributed (5 http + 5 grpc)", traces.length === 10, `traces=${traces.length}`);

  const sample = traces[0];
  const detail = await call("otel/getTrace", { traceId: sample.trace_id });
  check("trace detail resolves parent links", detail.spans?.length === 4 && detail.spans.filter((row) => row.parent_span_id).length === 3,
    JSON.stringify(detail.spans?.map((row) => [row.name, row.parent_span_id])));
  const attributes = detail.spans?.find((row) => row.name === "POST /checkout")?.attributes || {};
  check("http attributes survived", attributes["http.request.method"] === "POST" || attributes["http.response.status_code"] !== undefined,
    JSON.stringify(attributes));

  const logs = await call("otel/listLogs", { limit: 5 });
  const metrics = await call("otel/listMetrics", { limit: 5 });
  check("logs/metrics endpoints respond", Array.isArray(logs.logs) && Array.isArray(metrics.series), "shape");

  const analysis = await call("otel/analysisSql", {});
  check("analysis SQL exposes spans glob", !!analysis.csvGlob?.spans, JSON.stringify(analysis.csvGlob));
  console.log(`      CSV: ${analysis.csvGlob?.spans}`);
  console.log(`      data dir: ${dataDir}`);
} catch (error) {
  check("e2e run completed", false, error.message);
} finally {
  child.stdin.end();
  child.kill();
}

const failed = results.filter((item) => !item.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length ? 1 : 0);
