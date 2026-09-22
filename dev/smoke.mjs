// Drives the built sidecar exactly like the DBX host does: stdio JSON-RPC
// handshake, provider lifecycle, then real OTLP ingest over HTTP.
//
//   node dev/smoke.mjs [path/to/dbx-plugin-otel.exe]
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(here, "..");
const exe = process.argv[2] || path.join(root, "build", process.platform === "win32" ? "dbx-plugin-otel.exe" : "dbx-plugin-otel");
if (!existsSync(exe)) {
  console.error(`sidecar not found: ${exe}\nbuild it first (see build.mjs)`);
  process.exit(1);
}

const dataDir = mkdtempSync(path.join(tmpdir(), "dbx-otel-smoke-"));
process.env.DBX_PLUGIN_DATA_DIR = dataDir;

const child = spawn(exe, [], { stdio: ["pipe", "pipe", "inherit"], env: process.env });
const pending = new Map();
const events = [];
let buffer = "";
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
      if (message.error) reject(new Error(`${message.error.code}: ${message.error.message}`));
      else resolve(message.result);
    } else if (message.method) {
      events.push(message);
    }
  }
});

let nextID = 1;
function request(method, params, timeoutMs = 10000) {
  const id = nextID++;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${method} timed out`));
    }, timeoutMs);
    pending.set(id, {
      resolve: (value) => { clearTimeout(timer); resolve(value); },
      reject: (error) => { clearTimeout(timer); reject(error); },
    });
    child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
  });
}

const checks = [];
function check(name, condition, detail) {
  checks.push({ name, ok: !!condition, detail });
  console.log(`${condition ? "PASS" : "FAIL"}  ${name}${condition ? "" : `  -> ${detail}`}`);
}

function otlpJSON(service) {
  const start = BigInt(Date.now() - 1500) * 1000000n;
  const end = start + 420000000n;
  return {
    resourceSpans: [{
      resource: { attributes: [{ key: "service.name", value: { stringValue: service } }] },
      scopeSpans: [{
        spans: [{
          traceId: "b100000000000000000000000000beef",
          spanId: "00000000000000f1",
          name: "GET /health",
          kind: 2,
          startTimeUnixNano: start.toString(),
          endTimeUnixNano: end.toString(),
          attributes: [{ key: "http.status_code", value: { intValue: 200 } }],
          status: { code: 1 },
        }],
      }],
    }],
  };
}

async function main() {
  const handshake = await request("plugin/initialize", {
    host: { dbxVersion: "0.6.13", hostApiVersion: "1.1.0", protocolVersions: [1] },
    plugin: { id: "com.yiqiui.otel", version: "0.1.0" },
    permissions: ["host.events"],
  });
  check("handshake reports protocol v1", handshake.protocolVersion === 1, JSON.stringify(handshake));
  check("handshake echoes plugin id", handshake.plugin?.id === "com.yiqiui.otel", JSON.stringify(handshake));
  check("handshake advertises connections+events",
    handshake.capabilities?.includes("connections") && handshake.capabilities?.includes("events"),
    JSON.stringify(handshake.capabilities));

  const lifecycle = {
    provider: { id: "com.yiqiui.otel.receiver", databaseType: "otel" },
    connection: {
      id: "smoke-connection", db_type: "plugin", name: "smoke",
      external_config: { listen_host: "127.0.0.1", listen_port: 4318, retention_days: 7, autostart: true },
    },
    runtime: { host: "127.0.0.1", port: 0 },
  };

  const tested = await request("connection/test", lifecycle);
  check("connection/test succeeds", tested.success === true, JSON.stringify(tested));

  const connected = await request("connection/connect", lifecycle);
  check("connect starts receiver", connected.listening === true, JSON.stringify(connected));
  check("connect reports an endpoint", /^http:\/\/127\.0\.0\.1:\d+$/.test(connected.endpoint || ""), connected.endpoint);

  const endpoint = connected.endpoint;
  const response = await fetch(`${endpoint}/v1/traces`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(otlpJSON("smoke-svc")),
  });
  check("OTLP/HTTP JSON ingest accepted", response.status === 200, `status ${response.status}`);

  const sampled = await request("otel/sample", {});
  check("demo trace ingested", sampled.ingestedSpans === 4, JSON.stringify(sampled));

  const listed = await request("otel/listTraces", { limit: 20 });
  const services = new Set((listed.traces || []).flatMap((row) => row.services || []));
  check("listTraces sees the OTLP service", services.has("smoke-svc"), [...services].join(","));
  check("listTraces sees the demo service", services.has("demo-checkout"), [...services].join(","));

  const smokeTrace = (listed.traces || []).find((row) => (row.services || []).includes("smoke-svc"));
  check("smoke trace exposes duration", smokeTrace && Number(smokeTrace.duration_ns) === 420000000, JSON.stringify(smokeTrace));
  check("smoke trace root resolved", smokeTrace && smokeTrace.root_name === "GET /health", JSON.stringify(smokeTrace));

  const detail = await request("otel/getTrace", { traceId: smokeTrace.trace_id });
  check("getTrace returns spans", detail.spans?.length === 1, JSON.stringify(detail).slice(0, 200));
  check("span attributes decoded", detail.spans?.[0]?.attributes?.["http.status_code"] === 200,
    JSON.stringify(detail.spans?.[0]?.attributes));

  const analysis = await request("otel/analysisSql", {});
  const csvDir = path.join(dataDir, "otel", "csv", "spans");
  const csvFiles = readdirSync(csvDir).filter((name) => name.endsWith(".csv"));
  check("CSV files written per day", csvFiles.length > 0, csvFiles.join(","));
  const csv = readFileSync(path.join(csvDir, csvFiles[0]), "utf8");
  check("CSV carries both services", csv.includes("smoke-svc") && csv.includes("demo-checkout"), csv.slice(0, 200));
  check("analysis SQL references the real path", JSON.stringify(analysis).includes(dataDir.replace(/\\/g, "/")) || JSON.stringify(analysis).includes(dataDir),
    JSON.stringify(analysis.csvGlob));

  const logs = await request("otel/listLogs", { limit: 10 });
  check("demo logs listed", logs.count >= 1, JSON.stringify(logs).slice(0, 160));

  const metrics = await request("otel/listMetrics", { limit: 10 });
  check("demo metrics listed", metrics.count >= 1, JSON.stringify(metrics).slice(0, 160));

  const stopped = await request("connection/disconnect", lifecycle);
  check("disconnect stops receiver", stopped.listening === false, JSON.stringify(stopped));

  check("live ingest event was emitted", events.some((event) => event.method === "otel/ingested"),
    events.map((event) => event.method).join(","));

  child.stdin.end();
}

main()
  .catch((error) => {
    check("smoke run completed", false, error.message);
  })
  .finally(() => {
    const failed = checks.filter((item) => !item.ok);
    console.log(`\n${checks.length - failed.length}/${checks.length} checks passed`);
    child.kill();
    process.exit(failed.length ? 1 : 0);
  });
