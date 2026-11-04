# 请求耗时证据

本功能只观察时间与计数，不改变转发、重试、增量、压缩策略，也不修改 UI 的首字统计模式。

## 存在哪里

- HTTP 请求完成日志增加 `openai_response_id`，与既有 `request_id`、`client_request_id` 直接关联。
- 原生 WS 每轮完成时输出一条精简关联日志，包含 `turn`、`response_id`、账号和尝试次数。无需等长连接关闭。
- 额外耗时摘要仅写入应用日志目录下的 `request-timing/timing-*.jsonl`，不复制到日志数据库或标准输出。
- 触发条件：首次内容等待至少 10 秒；未收到内容且耗时至少 10 秒；HTTP/流内错误、传输/下游错误、缺少终止事件；或发生多次尝试。
- 正常快速请求不增加摘要文件记录。每条 HTTP 请求或 WS 轮次只完成一次摘要；重试共用记录器。

服务器启动时创建专用目录。目录权限 `0700`、文件 `0600`（Linux）。日志文件**内容合计不超过 50 MiB**，文件系统元数据另有少量开销；达到上限先淘汰最旧段。单条摘要最多 16 KiB，每轮最多保留 8 次尝试（第一次和最近七次），超出部分明确记 `omitted_attempts`，不会静默当成完整历史。

最多保留 7 天，每分钟清理；写入时也会清理。按不超过 1 小时/1 MiB 的分段淘汰，保留期不是保证：容量紧张时旧记录可能提前淘汰。重启重新计算存量，空闲时也会清理。只管理符合专用命名规则的文件。

记录写入失败不会中断用户请求；每分钟最多一条不含原始错误文本的告警。记录器不会保存正文、工具结果、图片、请求头、代理地址、凭据或密文。类别、阶段、失败原因均来自代码生成的标记，响应内容只用于即时分类和计数。

## 如何读时间

全部 `*_ms` 时间点以本次 HTTP 入口/WS 轮次开始为零点，包含此前尝试耗时。下一 WS 轮次重置零点，不计入轮次之间的用户等待。`acquire_ms`、`queue_ms` 和连接年龄是时长；队列等待属于连接获取的一部分，不能重复相加。

| 字段 | 含义 |
| --- | --- |
| `request_id` / `client_request_id` / `turn` | 网关请求、客户端关联 ID、WS 轮次 |
| `attempts[].response_id` / `upstream_request_id` | 各次尝试的 Responses ID / HTTP 上游请求 ID |
| `account_id` / `transport` / `number` | 该次尝试使用的账号、传输方式、尝试序号 |
| `start_ms` / `connected_ms` / `sent_ms` / `headers_ms` | 开始尝试、连接就绪、写请求返回、收到 HTTP 响应头 |
| `first_event_ms` | 网关读到并解析首个上游事件；并非内容首字 |
| `first_visible_ms` / `first_visible_type` | 首次可用内容及类别，如正文、推理内容、工具输入或图片 |
| `first_text_ms` | 首次正文文本事件；工具参数不计入 |
| `first_content_write_ms` | 包含首个内容事件的数据写入下游返回 |
| `first_content_flush_return_ms` | 含该内容的服务端 Flush/WS Write 返回；不证明客户端已收到或渲染 |
| `terminal_ms` / `end_ms` / `duration_ms` | 终止事件、该次尝试结束、整轮结束 |
| `events` / `event_bytes` | 已观察 JSON 事件数量/字节数，包括结构事件和最终事件 |
| `content_events` / `content_event_bytes` | 含内容的非终止事件数量/整个事件字节数，**不是**文本 token 数或去重文本长度 |
| `max_event_gap_ms` / `max_content_gap_ms` | 相邻事件/内容事件在网关处理时的最大间隔，不含首事件之前的等待 |
| `outbound_json_bytes` | 能在最终序列化点观察到的出站 JSON 大小，WS 在压缩前计数 |
| `http_encoded_body_bytes` | Codex HTTP 压缩后的请求体大小，不含 HTTP/TLS 帧开销 |
| `connection_reused` / `connection_age_ms` / `incremental` / `incremental_reason` | 连接复用、年龄、网关自动增量选择及原因；是否成功仍须结合终止结果 |
| `continuation_anchor` | 原生 WS 是否带 `previous_response_id`；仅带锚点不等于证明历史已经缩短，因此不会据此把 `incremental` 标成 true |
| `outcome` / `failure` / `last_stage` / `retry_reason` | 终止结果、独立的传输或下游失败标记、最后阶段、已知重试原因 |

缺失字段表示没有观察到，不能当成 0。部分插件或兼容传输不提供写完成/连接/最终序列化回调，这些字段留空。只有终止帧含完整答案时不捏造独立的首次内容时间。HTTP/2 请求上传与响应可并行，时间点不保证严格串行。

覆盖 HTTP/SSE（普通与透传）、HTTP→WS、原生 WS 连接池、WS 透传和 WS→HTTP 桥接。WS 透传后续轮次从收到 `response.create` 开始记录，包含准入处理，不含此前的客户端空闲。

v0.2.56 起，Chat Completions → Responses → Chat Completions 的流式转换也接入上述观察器，仍使用 HTTP/SSE。该分支的 `first_token_ms` 遵守 HTTP 的 `visible` / `semantic` 设置；`visible` 可包含推理内容或工具输入，并不等同于正文。结构事件、空增量、无效 JSON、仅终止事件不会伪造 visible 首字。转换后的内容实际写出及 Flush 返回单独计时，包括被暂存后释放的内容。

## HTTP 与 WS 的首字口径

按实际出站到上游的协议区分，不按客户端入口或用量表的 WS 标记猜测：

- HTTP/SSE 默认 `semantic`：跳过 `response.created`、`response.queued`、`response.in_progress` 等开始处理通知，首次语义事件记时；空的 `response.output_item.added` 也可能触发。仍可显式选择 `visible`。
- WS 固定使用 `ws_semantic`：收到类型明确的 reasoning item，或 summary_text / reasoning_text part 即记时，即使正文尚空；非空字符串的推理、正文、工具或媒体内容也可触发。不受普通 HTTP 设置影响。开始处理通知、心跳、未知事件、空/null/对象等非法 delta 不触发；单独的终止帧不填首字。
- HTTP 入口转 WS 上游使用 WS 口径；原生 WS 入口桥接 HTTP 上游也使用 WS semantic（按实际出站 HTTP、计时口径 ws_semantic 单独分组）。后续 WS 轮次分别重新计时。
- 已移除将开始处理通知计作首字的 `first_event` 选项。旧配置读取时归一化为 `semantic`，新写入只接受 `semantic` / `visible`。历史用量记录不重算。

### 单账号 HTTP 首次响应例外

按用户要求，仅为 #12140 准备启用 `accounts.extra.openai_http_ttft_mode = "first_event"`。此字段只作用于显式配置的 OpenAI API Key 账号的普通 HTTP 流式 Responses 链路（包括 Responses 转 Chat 输出）；没有配置的账号继续使用全局口径。代码没有硬编码账号 ID，全局设置仍不接受 `first_event`。发布部署后需通过管理员账号更新接口仅合并此账号的该字段，不覆盖其余 extra，也不修改全局设置。

此口径从该次转发开始到收到第一个有效、非终止的 Responses 事件记时，包含 `response.created` / `response.in_progress` 等处理通知。它不证明推理已经开始；错误、额度通知、心跳和单独终止事件不填首字。每次请求重新计时，不固定写入 2.581 秒，也不回填历史用量。摘要里的 `first_event_ms` 从请求入口计时，因此可能比用量首字多出转发前准备时间。

首内容、首正文、实际写出和 Flush 诊断仍分别记录；账号性能观察将 `first_event` 单独分组，不与 `semantic` 混算。该设置不改变首输出超时、切号、缓冲或响应提交。WS 入口/出口以及 WS→HTTP 回退仍按 `ws_semantic`，即使选中此账号也不使用例外。

WS semantic 是首次推理活动或内容信号，不是严格的正文可见时间。例如推理条目在 1.2 秒到达、正文在 10 秒到达，首字为 1.2 秒，正文里程碑仍为 10 秒。该规则参考 ranxi2001/sub2api v2.19.4；旧版本与本版本的 WS 首字不能直接混作同口径基线。独立的 `first_event_ms`、`first_visible_ms`、`first_text_ms` 诊断里程碑继续分别记录。诊断字段 `first_event_ms` 不是已移除的统计选项。计时判定不参与响应提交、重试、超时或刷出决策。

仅已接入事件观察器的链路创建耗时尝试。Chat 非流式缓冲转换、其他未接入的兼容路径和账号用量查询仍只保留既有日志，不会因缺少 Responses 终止事件而记为 `incomplete`。v0.2.53 曾错误纳入这些调用；调查该版本期间的摘要时，须结合入口路径确认，不能仅凭这类摘要认定请求失败。v0.2.54 已限制采集范围。

这些时间只能定位网关准备/获取连接/发送/缓冲与等待上游内容的边界。不能将全部上游等待归因为推理，也不能分辨上游内部队列或客户端渲染延迟。

## 下次如何查

通过既定 SSH 别名连接服务器，在实际应用日志目录中搜索目标 ID：

```sh
rg -F 'resp_要查询的ID' /实际日志目录/request-timing -g 'timing-*.jsonl'
```

也可用网关 UUID 或 `client_request_id` 搜索，摘要内包含各次尝试，不需要按时间猜测关联。找不到时同时检查普通完成日志：请求可能没有达到留证条件，或旧记录已按期限/容量淘汰。

明确要长期保留某次异常时，导出匹配的单条 JSON 到管理员本地调查目录；只保留一次，不复制整个日志目录，不启用正文抓取。导出的文件不在服务器自动淘汰范围。此功能不自动替用户永久保留所有异常。

## v0.2.56 完成日志中的精简元数据

即使快速请求不生成单独摘要，完成日志也保留最终尝试的 `openai_timing_first_event_ms`、`openai_timing_first_visible_ms`、`openai_timing_first_text_ms`、`openai_timing_first_content_write_ms`、`openai_timing_first_content_flush_return_ms`。这些时间沿用入口/WS 轮次的零点，缺失即未观察到；后续轮次不会继承旧值。服务端 Flush 返回仍不代表客户端已渲染。

HTTP→WS 完成日志额外记录：

- `ops_openai_ws_conn_id`、`openai_ws_preferred_conn_id`：实际连接与绑定指向的连接，可关联连接池日志。
- `openai_ws_scope_hash`、`openai_ws_scope_source`、`openai_ws_execution_lane`：原始线程/显式会话作用域的摘要和固定用途分类。记录不含真实线程标识或任意客户端 request-kind 值；未知用途统一记为 `other`，实际隔离仍使用完整原始作用域。
- `openai_ws_binding_status`：`hit`、`expired`、`not_present`、`missing_scope` 等。已被先前清理的绑定只能观察为 `not_present`，不猜测历史原因。
- `openai_ws_preferred_status`：首选连接是否可用、忙碌、握手条件变化或已不在该账号池。
- `openai_ws_acquire_reason`：复用首选/其他连接、新会话绑定缺失、寿命轮换、恢复等代码生成的原因。
- `openai_ws_checkpoint_before` / `openai_ws_checkpoint_after`：`ready`、`expired`、`no_completed_turn`、`upstream_write`、`ineligible_input`、`incomplete_output`、`downstream_disconnected`、`turn_failed`；配合既有 `openai_ws_auto_incremental_reason` 判断压缩边界、历史/参数变化。

连接池的 `connection_acquired` / `connection_evicted` 日志覆盖借出、新建和淘汰，包含连接 ID、账号、复用标记、原因、空闲时长/年龄及是否带检查点等元数据。寿命轮换、空闲额度、无保活连接超时、健康探测失败、上游关闭、账号清理和失败轮次分别标记。没有记录请求/回答正文、工具参数、凭据或原始错误消息。
