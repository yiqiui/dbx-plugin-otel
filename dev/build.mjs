// Build the sidecar for the current host platform into ./build.
// The DBX Go SDK has no published module tag yet, so backend/go.mod carries a
// `replace` pointing at a local t8y2/dbx checkout; override it with
// DBX_REPO=<path> if your clone lives elsewhere.
import { spawnSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const out = path.join(root, "build");
mkdirSync(out, { recursive: true });

const targets = {
  win32: { goos: "windows", goarch: process.arch === "arm64" ? "arm64" : "amd64" },
  darwin: { goos: "darwin", goarch: process.arch === "arm64" ? "arm64" : "amd64" },
  linux: { goos: "linux", goarch: process.arch === "arm64" ? "arm64" : "amd64" },
};
const target = targets[process.platform];
if (!target) throw new Error(`unsupported platform ${process.platform}`);

const name = process.platform === "win32" ? "dbx-plugin-otel.exe" : "dbx-plugin-otel";
// GOOS/GOARCH must be set explicitly: a developer machine may pin a different
// default in `go env` (cross-compiling setups are common), and `go test`/`go
// build` would then produce a binary the local host cannot run.
const result = spawnSync("go", ["build", "-o", path.join(out, name), "."], {
  cwd: path.join(root, "backend"),
  env: { ...process.env, CGO_ENABLED: "0", GOOS: target.goos, GOARCH: target.goarch },
  stdio: "inherit",
});
process.exit(result.status ?? 1);
