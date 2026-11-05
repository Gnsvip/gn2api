# Responses 余额耗尽与监控分类修复

本次修复不改变余额门槛、在途预留或结算逻辑。普通 HTTP、非流式请求、compact 和普通 WS 握手在站内余额不足时继续返回 HTTP **403**，`error.type` 和 `error.code` 均为 `insufficient_quota`。固定 Key 配额、RPM、Key 时间窗限流及真正上游的 429 保持原有行为。

用户已批准 Codex Responses 流式请求的服务端兼容例外：已认证且确认站内余额不足后，HTTP 建立 **200 SSE** 并发送一条 `response.failed`，其中 `response.status=failed`、`response.error.type/code=insufficient_quota`；WS 完成本地 **101** 握手，读取并丢弃首帧后发送同样的失败事件，以 1008 关闭。已连接 WS 后续轮次和排队/预留后发现余额不足也使用这个终止事件。它不代表成功生成，不进入账号调度、不调用上游、不产生模型用量；有余额的请求继续原有转发、上游重试和切号逻辑。

兼容范围仅限官方 Codex UA/originator，以及已观察到的 `codex-router/` UA，端点仅含 Responses 及其别名，不含 compact。鉴权层 HTTP 通过 `Accept: text/event-stream` 识别流式协商，不读取欠费请求的大段历史；handler 已解析出的流式标志也可用于排队后的拒绝。此识别只选择错误格式，不改变官方客户端鉴权白名单。WS 首帧沿用既有大小限制、最多等待 5 秒，丢弃内容而不保存正文；失败事件写入最多等待 2 秒，之后进入有界关闭流程。

HTTP 余额/固定 Key 额度耗尽响应增加 `x-should-retry: false`。官方 Python/Node SDK 默认会重试 429，即使正文已是 `insufficient_quota`；该标记让它们在本次调用中立即返回错误。浏览器 CORS 暴露此响应头；普通 WS 的 `error` 事件也携带同一标记。Codex 流及已经开始的 SSE 依靠 `response.failed` 终止。此修复不增加用户冷却、停用 Key 或封 IP；用户主动再次发送仍收到新的余额不足错误。

v0.2.64/v0.2.65 将站内余额错误改成 429，违反了站内余额不足保留 403 的要求，已在 v0.2.66 恢复。当前例外是用户随后批准的 Codex 流内失败协议，普通请求仍为 403，不再借用上游限流 429。

官方 Linux `codex-cli 0.166.0` 收到原有 `403 + insufficient_quota + x-should-retry: false` 后，单次 HTTP 请求会自动重连 5 次（共 6 次请求）；WS 握手拒绝后还会回退 HTTP。这是本次增加兼容格式的原因。禁止重试标记能控制官方 Python/Node SDK，但不能控制该版本 Codex 的会话级重连。`response.failed + insufficient_quota` 则会被 Codex 解释为终止额度错误（`usageLimitExceeded`），客户端使用自己的固定额度不足文案。

Responses 的 OpenAI 与兼容网关两个 handler 均使用同一错误构造器。非 Responses 端点保留原协议错误格式。首字规则、前端和账号调度策略不在本次修改范围内。

Ops 将明确余额/额度错误记作 `billing_error`，本地拒绝归用户侧业务限制。Codex 兼容拒绝记录逻辑状态 **403**，错误详情另存 `transport_status: 200/101`；不计为成功推理，也不沿用上一轮 WS 的上游账号或错误归因。真正上游错误保留原有归因。监控的 Go 与 SQL 分类都补认 `Insufficient account balance`、`INSUFFICIENT_BALANCE`、`insufficient_quota` 和 `API_KEY_QUOTA_EXHAUSTED`。

v0.2.67 线上验收发现，HTTP 的终止 SSE 被通用捕获分支先识别，绕过本地余额标记而误记 502；v0.2.68 令明确的本地余额拒绝标记优先，并补上完整 `OpsErrorLoggerMiddleware` 链路回归。只测独立日志构造器不能覆盖该分支顺序问题。

这是 v1 既有 `quota_or_balance` 类别的漏识别修正，未改变类别定义或判定优先级，因此保留 taxonomy version 1。历史重算必须单独执行，不能靠提升版本号使旧数据退出查询。

## 验证

- 定向回归覆盖余额不足的 HTTP/WS/compact 拒绝、已提交 SSE 错误、监控归属、固定 Key 配额和真正上游 429 切换逻辑。
- 原生测试使用官方正式 Linux Codex CLI 0.166.0 和实际鉴权中间件/错误构造器：HTTP 只请求一次；WS 最多包含一次启动预热和一次用户请求，零 HTTP 回退，明确禁止 Reconnecting。此前 429 响应通过的测试不能用于证明当前格式。
- 长驻 `app-server --stdio` 用官方 VSCode 宿主标识初始化，主动发送两轮后保持进程存活 60 秒，断言每轮只有一个 `willRetry:false` 额度错误，观察期无新增请求。自定义测试宿主名称会改变原生请求身份，应按普通客户端返回 403；不能通过放宽身份识别或次数断言掩盖这种测试条件差异。
- WS passthrough、ctx_pool、http_bridge 均覆盖“有余额首轮正常执行，结算后余额不足，下一轮返回失败且不再调用上游/扣费”；普通 WS 继续原有 `error` 帧，Codex 使用 `response.failed`。
- 余额恢复后可继续进入原转发链路；隔离用例关闭鉴权缓存以验证不存在新增冷却，生产仍依赖既有余额缓存失效机制。真正的上游 429、401 后换号和上下文续传继续回归验证。
- 官方 Node SDK 7.51.0、Python SDK 3.46.0 使用默认重试设置，同一个 client 主动发送两次：403 余额响应应共 2 次请求、2 次错误；保留旧版 429 无标记作为对照共 6 次请求，真正限流对照仍共 6 次请求。均覆盖流式和非流式调用。
- 测试不会调用模型、读取用户 Codex 配置或消耗真实账号额度。任意外层脚本仍可能忽略错误自行继续请求，服务端的错误码无法强制终止外层循环。

原生验证命令（需设置 `GN2API_TEST_CODEX_EXE` 为本机官方可执行文件）：

```text
go test -tags unit ./internal/server/middleware ./internal/handler -run 'Test(APIKeyBalanceNativeCodex|OpenAIWSBillingErrorNativeCodex|ResponsesBillingNativeCodex)' -count=1 -timeout=240s -v
```

SDK 验证需设置 `GN2API_TEST_NODE`、`GN2API_TEST_OPENAI_NODE`（独立安装的 `openai` 包路径）、`GN2API_TEST_PYTHON` 和对应的 `PYTHONPATH`，运行 `TestAPIKeyBalanceOfficialSDKNoRetry`。测试依赖不加入产品构建依赖。

## 历史分类修复

`repair_balance_error_categories.py` **只生成 SQL，不连接数据库**；默认生成只读预览。时间必须包含时区，每次最多 7 天；结束时间应至少落后当前 10 分钟，避开活跃请求。先用新代码生成 SQL 并预览，核对后才使用 `--apply` 生成修改事务，交由 psql 执行。

```text
python deploy/repair_balance_error_categories.py --start 2026-11-05T00:00:00Z --end 2026-11-05T14:40:00Z
```

工具使用网关原始 SQL 的请求去重和分类规则，逐个核对完整统计桶的旧分类数量。只有旧分类全部数量与留存原始日志吻合时才重写分类：

- `repair`：证据完整，可修正。
- `already_correct`：已正确，无需再次修改。
- `skip_source_mismatch`：原始来源、去重或统计数量不一致，保留现状。
- 没有留存来源、超出范围或只有部分时间覆盖的桶不修改。

修改事务锁定两张错误分类聚合表，限制锁等待和语句时间；失败整体回滚。只修正错误分类数量，不修改原始日志、总请求数、成功数、用量、用户余额、延迟统计或聚合游标。修正后可命中既有的“余额/额度”健康评分排除规则；**全部请求成功率仍按真实成功数/请求数计算**。

可用独立安装的 `@electric-sql/pglite` 验证真实 PostgreSQL 执行。设置 `GN2API_PGLITE_PATH` 和 `GN2API_PYTHON`，执行：

```text
node deploy/repair_balance_error_categories_test.cjs
```

回归覆盖重复请求去重、五种时间粒度、真正权限/限流错误保留、总数守恒、来源缺失保护、只读预览和重复修复幂等性。该依赖只用于隔离测试，不加入项目构建依赖。
