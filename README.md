# OpenTelemetry — DBX 插件

在 DBX 里接收、浏览并分析 OpenTelemetry 遥测数据。

- **接收**：插件 Sidecar（Go）内置 OTLP 接收端，同时支持 **OTLP/HTTP**（`/v1/traces`、`/v1/metrics`、`/v1/logs`，`application/x-protobuf` 与 `application/json`）与 **OTLP/gRPC**（默认 4317，可设为 0 关闭）。
- **存储**：按天分片双写。JSONL 供插件自身查询；CSV 供 DBX 内置 DuckDB 直接 `read_csv_auto` 做重分析。
- **浏览**：工作台（`workbench`）提供接收端状态、链路列表、span 瀑布图、指标与日志。
- **分析**：结果视图（`result-view`）把任意含 span 列的查询结果渲染成瀑布图；“SQL 分析”页给出已填好真实路径的 DuckDB 语句。

```
应用 SDK ──OTLP/HTTP──▶ 插件 Sidecar ──▶ otel/csv/<signal>/<date>.csv ──▶ DBX DuckDB 连接 + SQL
                              └────────▶ otel/jsonl/<signal>/<date>.jsonl ─▶ 工作台列表/瀑布图
```

## 环境要求

| 依赖 | 说明 |
| --- | --- |
| Go 1.22+ | `backend/go.mod` 要求 1.22。若本机是 1.21，`GOTOOLCHAIN=auto` 会自动下载所需工具链，无需手动升级 |
| Node.js 22+ | 跑 `@dbx-app/plugin-cli` 与本地 dev 宿主 |

Go SDK 直接按**伪版本**远程引用：

```
github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk v0.0.0-20260922092522-e1fc28ab861d
```

Go SDK 目前没有发布 tag（`t8y2/dbx` 只有 `plugin-cli-v*` 与 `v0.6.x`），`@v0.1.0` 这类版本号会报 `unknown revision`；`@main` 解析出的伪版本写进 `go.mod`/`go.sum` 后就是固定提交，因此本地与 CI 都不需要 checkout dbx，也不需要 `DBX_PLUGIN_SDK_ROOT`。

## 常用命令

```bash
# 单元测试 + 集成测试（接收、解码、落盘、聚合、端口回退、鉴权、保留期）
cd backend && go test ./... -v

# 构建当前平台的 sidecar 到 build/
node dev/build.mjs

# 按 DBX 宿主的真实顺序驱动 sidecar：stdio 握手 → 连接生命周期 → OTLP 上报 → 查询 → 落盘校验
node dev/smoke.mjs

# 端到端：真实 OTel Go SDK 示例应用分别经 HTTP 与 gRPC 打进 sidecar，并断言聚合结果
node dev/e2e.mjs

# 浏览器预览工作台 UI（不需要安装 DBX 插件；在项目目录内执行，不带路径参数）
dbx-plugin dev

# 打包 .dbxp
dbx-plugin package .

# 校验包结构（entrypoint 路径、checksum 覆盖率与 SHA-256）
python dev/verify-package.py
```

产物：`dist/com.yiqiui.otel-<version>-<target>.dbxp`（未签名审阅候选）+ 同名 `.artifact.json`。

### Windows 上的三个坑（脚本里已处理）

1. **`go env` 的全局值会污染构建**。若本机 `go env -w` 写过 `GOOS=linux`（交叉编译到服务器很常见），`go build`/`go test`/`go run` 都会产出或编译成本机跑不了的产物，并在 `runtime/cgo` 报 `unknown type name 'sigset_t'`。**每一处**派生 Go 命令的地方都要显式设置 `GOOS/GOARCH/CGO_ENABLED`——`dev/build.mjs` 与 `dev/e2e.mjs` 里的 `go run` 都踩过这个。
2. **Go SDK 无发布 tag**：`@v0.1.0` 会报 `unknown revision`，必须用 `@main` 解析出的伪版本（见上）。另外 `.github/workflows` 里模板给的复用工作流 ref `@plugin-sdk-v1` 在 `t8y2/dbx` 中**并不存在**，CI 会直接解析失败，已改成 `@main`。
3. **`otlptracehttp.WithEndpointURL` 不会补 `/v1/traces`**：直接传 `http://127.0.0.1:4318` 会 POST 到根路径并拿到 404。`examples/checkout-service` 里已按路径是否存在再决定拼接。

## 安装到 DBX

1. Plugin Center → 设置 → 打开「允许安装未签名插件（开发模式）」；
2. 本地安装 `dist/com.yiqiui.otel-0.1.0-windows-x64.dbxp`；
3. 新建连接 → 选择 **OpenTelemetry 接收端**，填监听地址与端口（默认 `127.0.0.1:4318`）、保留天数、可选上报令牌；
4. 连接后打开工作台，点「启动接收端」或「注入演示链路」即可看到数据。

应用侧配置（以 OTLP/HTTP 为例，多数 SDK 用环境变量即可）：

```bash
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
```

设置了上报令牌时，导出器需附加请求头 `Authorization: Bearer <token>`（或 `X-OTLP-Token`）。

## 用 SQL 做重分析

工作台的「SQL 分析」页会给出真实 CSV 路径与现成语句。在 DBX 里新建一个 **DuckDB 文件连接**，直接执行：

```sql
-- 最慢的 20 条 trace
SELECT trace_id,
       arg_min(name, start_unix_nano) AS root_name,
       COUNT(*) AS spans,
       MAX(end_unix_nano) - MIN(start_unix_nano) AS duration_ns
FROM read_csv_auto('C:/Users/<you>/.../otel/csv/spans/*.csv')
GROUP BY trace_id
ORDER BY duration_ns DESC
LIMIT 20;
```

`attributes_json` / `resource_attributes_json` 是 JSON 字符串，用 `json_extract(attributes_json, '$."http.route"')` 取字段。

## 设计取舍

- **接收端必须在 Sidecar**：DBX 工作台 iframe 是 `sandbox="allow-scripts"` 且默认完全禁网，遥测不可能从 UI 进来。
- **端口冲突自动回退**：4318 是机器级共享资源，本机常已有 collector 占用。回退到随机端口后，状态卡会明确显示「配置的端口被占用，实际监听在 …」，避免对着错误端口排障。
- **瀑布图两条数据源**：工作台走 Sidecar 查询（按天扫描，无宿主上限）；结果视图只能拿到宿主快照，**上限 500 行**，因此结果被截断时界面会提示改用 DuckDB。
- **CSV 而非 Parquet**：避免为一个插件引入 `arrow/parquet` 依赖链；DuckDB 读 CSV 已足够，文件也能被其他工具直接消费。需要列存时在 `store.go` 增加一路 writer 即可，不影响现有布局。
- **OTLP/HTTP + OTLP/gRPC**：gRPC 只在 Sidecar 二进制里引入 grpc-go，不进 DBX 主程序，因此不违反宿主「不增大基础包」的约束；把连接的 `grpc_port` 设为 0 即可退回单端口模式。

## 目录

```
backend/           Go sidecar：OTLP 接收、规范化、存储、查询、RPC 分发
  store.go           JSONL + CSV 双写、按天分片、保留期清理
  receiver.go        OTLP/HTTP 端点、端口回退、令牌校验
  grpc.go            OTLP/gRPC 三个导出服务，与 HTTP 共用同一存储
  normalize.go       OTLP proto → 扁平行（span / metric / log）、trace 聚合
  query.go           trace 列表、指标/日志列表、DuckDB SQL 生成、演示数据
  main.go            插件生命周期与 RPC 路由
  *_test.go          集成测试（含用官方 OTel SDK 真实上报的两条）
ui/index.html      工作台 + 结果视图（同一入口，按 contributionId 分支）
dev/               build.mjs / smoke.mjs / e2e.mjs / verify-package.py
examples/checkout-service/  可独立运行的真实应用（HTTP 与 gRPC 两种协议）
manifest.json      插件清单（三个贡献点 + 中英本地化）
```

## 示例应用

`examples/checkout-service` 是一个用官方 OTel Go SDK 埋点的小服务，每次运行产生 5 条 4-span 链路（其中一条含错误 span）：

```bash
cd examples/checkout-service
go run .                                              # http/protobuf -> 127.0.0.1:4318
OTEL_EXPORTER_OTLP_PROTOCOL=grpc go run .             # grpc          -> 127.0.0.1:4317
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:54321 go run .   # 端口回退后指向真实端口
```

它是 `dev/e2e.mjs` 端到端验证的发送端，也可以直接对着已安装的插件跑，用来在工作台里看真实数据。

## 上架 dbx-store

官方流程是**一个 PR 搞定**（没有单独的 submission issue）：

1. 打 tag 并发布 GitHub Release（例如 `v0.2.0`），仓库内的 `.github/workflows/plugin-release.yml` 会在 5 个 runner 上各构建一个未签名 `.dbxp` + `.artifact.json`，并合并出 `release-candidates.json` 回传到该 Release；
2. 从 Release 里取每个 target 的 `url` / `sha256` / `size`（就是 `release-candidates.json` 的内容，无需手算）；
3. 复刻 `t8y2/dbx-store`，在 `candidates/com.yiqiui.otel.json` 写入候选元数据（`schemaVersion: 1`、`id`、`publisher`、`version`、`name`、`description`、`icon`、`tags`、`permissions`、`source` 指向**带 tag 的源码链接**、`homepage`、`license`、`releaseNotes`、`localizations`、`targets`），向 `main` 开 PR 并填写模板；
4. CI 在校验通过后会保持红色并提示 `open candidate(s) awaiting DBX Store signing`——这是故意的，防止未签名内容被合并；
5. 维护者在该 PR 上运行受保护的 **Sign plugin PR candidates** 工作流：签名、把最终资产发布到 R2（`https://dl.dbxio.com/plugins/<id>/<version>/...`），并把生成的 `plugins/com.yiqiui.otel.json`、重建的 `catalog/index.json`、以及删除 `candidates/...` 提交回你的 PR 分支；
6. CI 变绿后由维护者合并。

约束：未签名包的 `manifest.json` 必须与候选元数据的 `id`/`version`/`publisher` 完全一致；URL 必须是 HTTPS 且不能指向 `t8y2/dbx-store` 的 release；已上架或已吊销的版本不能重复提交；**同一版本不允许原地重建**（改了任何字节都要升版本）。`publishers/yiqiui.json` 已存在，再次提交无需重复添加。

插件源码与未签名候选留在本仓库；不要把它们提交到 `t8y2/dbx`，那个仓库只接收插件宿主、SDK、CLI、schema、文档与官方示例的改动。

完整规范见 `t8y2/dbx-store/CONTRIBUTING.md`、[plugin-development](https://dbxio.com/cn/docs/plugin-development) 与 `plugins/README.md`。
