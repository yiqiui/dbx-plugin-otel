// One-off: run the sidecar the way the DBX host would (connect lifecycle) and
// keep it alive so remote OTLP exporters can push into it. Prints otel/status
// counts whenever they change. Usage: node dev/serve-real.mjs <exe> <dataDir>
// Stops when stdin closes or the process is killed.
import { spawn } from "node:child_process";

const [exe, dataDir] = process.argv.slice(2);
if (!exe || !dataDir) {
  console.error("usage: node dev/serve-real.mjs <exe> <dataDir>");
  process.exit(1);
}

const child = spawn(exe, [], { stdio: ["pipe", "pipe", "inherit"], env: { ...process.env, DBX_PLUGIN_DATA_DIR: dataDir } });
let buffer = "";
let lastCounts = "";
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
      console.log("[event]", message.method, JSON.stringify(message.params || {}).slice(0, 200));
    }
  }
});

const pending = new Map();
let nextID = 1;
function request(method, params, timeoutMs = 15000) {
  const id = nextID++;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`${method} timed out`)); }, timeoutMs);
    pending.set(id, { resolve: (v) => { clearTimeout(timer); resolve(v); }, reject });
    child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
  });
}

async function main() {
  const test = await request("connection/test", {
    connection: { id: "verify-real", external_config: { listen_host: "0.0.0.0", listen_port: 4318, grpc_port: 4317, retention_days: 7, autostart: true } },
  });
  console.log("[test]", JSON.stringify(test));
  const status = await request("connection/connect", {
    connection: { id: "verify-real", external_config: { listen_host: "0.0.0.0", listen_port: 4318, grpc_port: 4317, retention_days: 7, autostart: true } },
  });
  console.log("[connect]", JSON.stringify(status));
  setInterval(async () => {
    try {
      const current = await request("otel/status", {});
      const counts = JSON.stringify(current.counts);
      if (counts !== lastCounts) {
        lastCounts = counts;
        console.log("[status]", counts, current.endpoint, current.grpcEndpoint);
      }
    } catch (error) {
      console.error("[status-error]", error.message);
    }
  }, 3000).unref();
}

process.stdin.resume();
// Stay alive until the process is killed: under background shells stdin closes
// immediately, which would tear the receiver down before any data arrives.
process.stdin.on("end", () => console.log("[stdin] closed (ignored)"));
main().catch((error) => { console.error("[fatal]", error.message); child.kill(); process.exit(1); });
