# HTTP Responses 自动 WS 增量续传

普通客户端继续 POST `/v1/responses`。OAuth 账号已启用的 HTTP→WS 路径，在同一上游连接仍有效且历史相符时，自动发送 `previous_response_id` 与新增 input；客户端无需开启 `supports_websockets`。客户端与网关之间仍是原来的 HTTP/SSE。

## 采用增量的条件

- 已取得实际连接的独占租约，该连接上一轮成功完成，检查点未超过 10 分钟。
- 分组、API key、原始线程/会话执行作用域一致。
- 最终出站模型、推理等级、工具定义及其他非传输参数一致，上下文窗口未变化。
- 本轮完整历史的前缀等于上一轮完整输入加上游输出，且存在新增 input。
- 历史可自行还原。未解析的 item_reference、未知输出类型或缺少完整 terminal output 时采用全量。

比较保留消息正文、密文、工具名称/参数及 call_id；忽略输出对象 ID、status、已知内部 turn_id/create_time 和空 annotations/logprobs 等重放元数据。比较时的规范化不会修改实际发送的新 input。客户端显式提供 previous_response_id 的既有逻辑保持独立。

## 连接、并发与恢复

检查点直接属于连接，只含 SHA-256 摘要、条目数、响应标识与过期时间，不保存问答正文，不写数据库或 Redis。连接数量仍由现有账号连接池与并发限制管理。自动续传成功后的 session→connection 关联最多保留 10 分钟；原有其他用途的 response/account 索引 TTL 不做全局修改。

取得租约后才判断增量，因此不会把某连接的增量发送到另一连接。连接繁忙而调度到其他连接时发送全量；同一会话的并发分支也须重新比较历史。任何上游写操作（包括原生 WS、预热）都会清除旧检查点；只有成功 completed 且下游仍连接时才建立下一检查点。

自动补入的响应锚点失效时，未输出给客户端的请求可在现有重试预算内重新发送原始完整上下文，工具调用与工具结果均保留。WS 不支持等允许 HTTP 回退的条件，同样使用完整请求。已经开始向客户端输出时不自动重放，避免重复执行或重复回答。

这项改动作用于已有 HTTP Responses→WS 路径，不将所有端点强制改为 WS；Chat Completions 兼容端点、compact、显式禁用 WS、透传等保留各自传输规则。它优化传输与续传，不保证上游 prompt cache 命中率或模型能力。

## 可观察字段

现有 `http request completed` 日志增加 `openai_ws_auto_incremental` 和 `openai_ws_auto_incremental_reason`。字段反映本请求最终 WS 尝试；退回 HTTP 时为 false/http_fallback。无问答正文、缓存键原文或密文日志。

## 验证

- 本地真实 WebSocket 测试服务器覆盖连续三轮、JSON/SSE、function/custom tool 续传、旧响应失效、HTTP 全量回退、连接关闭、过期、API key/线程隔离及并发分支。
- 现有相关 service、handler、middleware 回归 874 项测试及子用例通过，0 失败、0 跳过。
- 测试命令：`go test -json -tags unit ./internal/service ./internal/handler ./internal/server/middleware -run 'Test.*(WS|WebSocket|HTTPToWS|Codex.*Replay|RequestAccessLogger)' -count=1 -timeout=240s`。
- 未发起生产模型请求，未修改线上服务。代码完成后仍需按发布流程上线，再用上述日志字段核对真实流量采用增量的比例与回退原因。没有将本地模拟上游测试称为官方线上端到端验收。
