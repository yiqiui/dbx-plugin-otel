// Ad-hoc: drive otel/* exactly like the workbench does (no saved connection).
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";

const root = path.join(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(\w:)/, "$1")), "..");
const exe = process.argv[2] || path.join(root, "build", "dbx-plugin-otel.exe");
const dataDir = mkdtempSync(path.join(tmpdir(), "dbx-otel-adhoc-"));
process.env.DBX_PLUGIN_DATA_DIR = dataDir;

const child = spawn(exe, [], { stdio: ["pipe", "pipe", "inherit"], env: process.env });
let buffer = "";
const pending = new Map();
child.stdout.on("data", (chunk) => {
  buffer += chunk.toString("utf8");
  let i;
  while ((i = buffer.indexOf("\n")) >= 0) {
    const line = buffer.slice(0, i).trim();
    buffer = buffer.slice(i + 1);
    if (!line) continue;
    const msg = JSON.parse(line);
    if (msg.id !== undefined && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      msg.error ? reject(new Error(JSON.stringify(msg.error))) : resolve(msg.result);
    }
  }
});
let id = 1;
const call = (method, params, ms = 8000) => new Promise((resolve, reject) => {
  const rid = id++;
  const timer = setTimeout(() => { pending.delete(rid); reject(new Error(method + " timed out")); }, ms);
  pending.set(rid, { resolve: (v) => { clearTimeout(timer); resolve(v); }, reject: (e) => { clearTimeout(timer); reject(e); } });
  child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id: rid, method, params }) + "\n");
});

const step = async (label, fn) => {
  try { console.log(`OK   ${label}:`, JSON.stringify(await fn()).slice(0, 220)); }
  catch (error) { console.log(`FAIL ${label}:`, error.message); }
};

await call("plugin/initialize", { host: { protocolVersions: [1] }, plugin: { id: "dev.yiqiui.otel", version: "0.1.0" }, permissions: [] });
await step("otel/status", () => call("otel/status", { connectionId: null }));
await step("otel/start (as UI clicks it)", () => call("otel/start", { connectionId: null }));
await step("otel/status after start", () => call("otel/status", { connectionId: null }));
await step("otel/sample", () => call("otel/sample", { connectionId: null }));
await step("otel/listTraces", () => call("otel/listTraces", { connectionId: null, limit: 5 }));
child.kill();
process.exit(0);
