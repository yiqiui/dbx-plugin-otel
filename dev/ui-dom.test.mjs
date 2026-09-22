// DOM-level regression test for the workbench: every control must actually
// issue its RPC. Guards against the addEventListener("Click") class of bug that
// silently leaves the whole UI dead inside the DBX sandbox.
//   npm install jsdom && node dev/ui-dom.test.mjs
import { readFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const here = path.dirname(fileURLToPath(import.meta.url));
const html = readFileSync(process.argv[2] || path.join(here, "..", "ui", "index.html"), "utf8");
const results = [];
const check = (name, ok, detail) => {
  results.push({ name, ok });
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${ok ? "" : "  -> " + detail}`);
};

const payload = (method) => {
  switch (method) {
    case "otel/status":
      return { listening: true, endpoint: "http://127.0.0.1:4318", requested: "127.0.0.1:4318", fallbackUsed: false,
        grpcListening: true, grpcEndpoint: "grpc://127.0.0.1:4317", grpcRequested: "127.0.0.1:4317", grpcFallback: false,
        counts: { spans: 7, metrics: 2, logs: 1 }, dataDir: "D:/data", authRequired: false };
    case "otel/listTraces":
      return { traces: [{ trace_id: "abc123def456abc1", root_name: "POST /checkout", service: "web", services: ["web"], span_count: 4, error_count: 1, duration_ns: 820000000, duration_readable: "820.00ms" }] };
    case "otel/getTrace":
      return { spans: [
        { span_id: "s1", parent_span_id: "", name: "POST /checkout", kind: "server", service: "web", status_code: "ok", start_unix_nano: 1000, end_unix_nano: 9000, duration_ns: 8000, attributes: {}, resource_attributes: {}, events: [], links: [] },
        { span_id: "s2", parent_span_id: "s1", name: "SELECT orders", kind: "client", service: "db", status_code: "error", start_unix_nano: 2000, end_unix_nano: 5000, duration_ns: 3000, attributes: {}, resource_attributes: {}, events: [], links: [] },
      ] };
    case "otel/listMetrics": return { series: [{ metric_name: "checkout.duration", kind: "histogram", service: "web", samples: 3, latest_value: 820, latest_time: "now" }] };
    case "otel/listLogs": return { logs: [{ timestamp: "now", severity_text: "ERROR", severity_number: 17, service: "web", body: "boom", trace_id: "abc" }] };
    case "otel/analysisSql": return { csvGlob: { spans: "D:/data/csv/spans/*.csv" }, queries: [{ title: "slowest", sql: "SELECT 1" }] };
    default: return {};
  }
};

function makeDom(contributionId, context) {
  const calls = [];
  let resolveReady;
  const dom = new JSDOM(html, {
    runScripts: "dangerously",
    url: "http://localhost/",
    beforeParse(window) {
      window.dbxPlugin = {
        ready: new Promise((resolve) => { resolveReady = resolve; }),
        locale: "zh-CN",
        theme: { appearance: "light", tokens: {} },
        capabilities: { downloadFile: false, planApi: false, storage: false },
        context,
        invoke: (method) => { calls.push(method); return Promise.resolve(payload(method)); },
        notify: () => Promise.resolve(),
        onEvent: () => {},
        onContext: () => {},
        storage: { get: () => Promise.resolve(null), set: () => Promise.resolve(), delete: () => Promise.resolve() },
      };
    },
  });
  // The host delivers contributionId in the init payload and only then resolves
  // ready; reproduce that order so the plugin can branch on it.
  const open = () => {
    dom.window.document.dispatchEvent(
      new dom.window.CustomEvent("dbx-plugin-init", { detail: { contributionId, type: "init" } })
    );
    resolveReady();
  };
  return { dom, calls, open };
}

const settle = async (dom, rounds = 8) => {
  for (let index = 0; index < rounds; index += 1) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  return dom.window.document;
};

const byText = (document, selector, text) =>
  [...document.querySelectorAll(selector)].find((node) => node.textContent.trim() === text);

// ------------------------------------------------------------------ workbench

{
  const { dom, calls, open } = makeDom("dev.yiqiui.otel.workbench", { connectionId: "c1" });
  open();
  const document = await settle(dom);
  check("workbench renders status", document.body.textContent.includes("OpenTelemetry"), document.body.textContent.slice(0, 80));
  check("initial status fetched", calls.includes("otel/status"), calls.join(","));
  check("status shows the live endpoint", document.body.textContent.includes("http://127.0.0.1:4318"), "endpoint missing");
  check("status shows the gRPC endpoint", document.body.textContent.includes("grpc://127.0.0.1:4317"), "grpc endpoint missing");
  check("span count rendered", /7/.test(document.querySelector(".grid")?.textContent || ""), document.querySelector(".grid")?.textContent);

  const before = calls.length;
  byText(document, "button", "启动接收端")?.click();
  await settle(dom);
  check("start button issues otel/start", calls.slice(before).includes("otel/start"), calls.slice(before).join(","));

  const before2 = calls.length;
  byText(document, "button", "停止接收端")?.click();
  await settle(dom);
  check("stop button issues otel/stop", calls.slice(before2).includes("otel/stop"), calls.slice(before2).join(","));

  const before3 = calls.length;
  byText(document, "button", "按保留期清理")?.click();
  await settle(dom);
  check("purge button issues otel/purge", calls.slice(before3).includes("otel/purge"), calls.slice(before3).join(","));

  const before4 = calls.length;
  byText(document, "button", "注入演示链路")?.click();
  await settle(dom);
  check("sample button issues otel/sample", calls.slice(before4).includes("otel/sample"), calls.slice(before4).join(","));

  const before5 = calls.length;
  byText(document, ".tab", "指标")?.click();
  await settle(dom);
  check("metrics tab issues otel/listMetrics", calls.slice(before5).includes("otel/listMetrics"), calls.slice(before5).join(","));
  check("metrics table rendered", document.body.textContent.includes("checkout.duration"), "no metric row");

  const before6 = calls.length;
  byText(document, ".tab", "日志")?.click();
  await settle(dom);
  check("logs tab issues otel/listLogs", calls.slice(before6).includes("otel/listLogs"), calls.slice(before6).join(","));

  const before7 = calls.length;
  byText(document, ".tab", "SQL 分析（DuckDB）")?.click();
  await settle(dom);
  check("analyze tab issues otel/analysisSql", calls.slice(before7).includes("otel/analysisSql"), calls.slice(before7).join(","));
  check("analyze tab shows generated SQL", document.body.textContent.includes("SELECT 1"), "no sql block");

  // traces tab: row click must open the waterfall
  byText(document, ".tab", "链路")?.click();
  await settle(dom);
  const row = document.querySelector("tr.clickable");
  check("trace row rendered", !!row, "no rows");
  const before8 = calls.length;
  row?.click();
  await settle(dom);
  check("trace row click issues otel/getTrace", calls.slice(before8).includes("otel/getTrace"), calls.slice(before8).join(","));
  check("waterfall bars rendered", document.querySelectorAll(".wf-bar").length === 2, `${document.querySelectorAll(".wf-bar").length} bars`);
  check("errored span styled", !!document.querySelector(".wf-bar.error"), "no error bar class");

  // an invoke failure must surface inside the page, not via a blocked alert()
  dom.window.dbxPlugin.invoke = () => Promise.reject(new Error("boom-connection-closed"));
  byText(document, "button", "停止接收端")?.click();
  await settle(dom);
  check("RPC failure shown in page", document.body.textContent.includes("boom-connection-closed"), document.body.textContent.slice(0, 120));
  dom.window.close();
}

// ---------------------------------------------------------------- result-view

{
  const result = {
    columns: ["trace_id", "span_id", "parent_span_id", "name", "kind", "service", "status_code", "start_unix_nano", "end_unix_nano", "duration_ns"],
    rows: [
      ["t1", "s1", "", "POST /checkout", "server", "web", "ok", 1000, 9000, 8000],
      ["t1", "s2", "s1", "SELECT orders", "client", "db", "error", 2000, 5000, 3000],
      ["t1", "s3", "s1", "GET cache", "internal", "web", "unset", 6000, 6500, 500],
    ],
    truncated: false,
    sql: "SELECT * FROM spans",
    connectionId: "c1",
  };
  const { dom, open } = makeDom("dev.yiqiui.otel.waterfall", { connectionId: "c1", result });
  open();
  const document = await settle(dom);
  check("result-view renders waterfall", document.querySelectorAll(".wf-bar").length === 3, `${document.querySelectorAll(".wf-bar").length} bars`);
  check("result-view indents children", (document.querySelector(".wf-name")?.getAttribute("style") || "").includes("padding-left"), "no indent style");
  dom.window.close();
}

{
  const { dom, open } = makeDom("dev.yiqiui.otel.waterfall", { connectionId: "c1", result: { columns: ["a", "b"], rows: [[1, 2]], truncated: true } });
  open();
  const document = await settle(dom);
  check("result-view warns on truncated snapshot", document.body.textContent.includes("500"), document.body.textContent.slice(0, 160));
  check("result-view explains missing span columns", document.body.textContent.includes("trace_id"), "no hint");
  dom.window.close();
}

const failed = results.filter((item) => !item.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length ? 1 : 0);
