// One-off: spawn a sidecar against an existing data dir and print otel/status
// counts. Usage: node dev/status-probe.mjs <exe> <dataDir>
import { spawn } from "node:child_process";

const [exe, dataDir] = process.argv.slice(2);
if (!exe || !dataDir) {
  console.error("usage: node dev/status-probe.mjs <exe> <dataDir>");
  process.exit(1);
}

const child = spawn(exe, [], { stdio: ["pipe", "pipe", "inherit"], env: { ...process.env, DBX_PLUGIN_DATA_DIR: dataDir } });
let buffer = "";
child.stdout.on("data", (chunk) => {
  buffer += chunk.toString("utf8");
  let index;
  while ((index = buffer.indexOf("\n")) >= 0) {
    const line = buffer.slice(0, index).trim();
    buffer = buffer.slice(index + 1);
    if (!line) continue;
    const message = JSON.parse(line);
    if (message.id === 1) {
      const counts = message.result && message.result.counts;
      console.log(`${path(exe)} :: counts=${JSON.stringify(counts)} dataDir=${message.result && message.result.dataDir}`);
      child.stdin.end();
      child.kill();
      process.exit(0);
    }
  }
});
setTimeout(() => {
  console.error("timeout waiting for otel/status");
  child.kill();
  process.exit(1);
}, 10000).unref();
// give the process a moment to boot, then ask for status (no connect needed)
setTimeout(() => {
  child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "otel/status", params: {} })}\n`);
}, 400);

function path(value) {
  return value.replace(/^.*[\\/]/, "");
}
