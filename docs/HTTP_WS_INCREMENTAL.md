# HTTP Responses 自动 WS 增量续传

普通客户端继续 POST `/v1/responses`。OAuth 账号已启用的 HTTP→WS 路径，在同一上游连接仍有效且历史相符时，自动发送 `previous_response_id` 与新增 input；客户端无需开启 `supports_websockets`。客户端与网关之间仍是原来的 HTTP/SSE。

## 采用增量的条件

- 已取得实际连接的独占租约，该连接上一轮成功完成，检查点未超过 10 分钟。
- 分组、API key、原始线程/会话执行作用域一致。
- 最终出站模型、推理等级、工具定义及其他非传输参数一致，上下文窗口未变化。
- 本轮完整历史的前缀等于上一轮完整输入加上游输出，且存在新增 input。
- 历史可自行还原。未解析的 item_reference、未知输出类型或缺少完整 terminal output 时采用全量。

比较保留消息正文、密文、工具名称/参数及 call_id；忽略输出对象 ID、status、已知内部 turn_id/create_time 和空 annotations/logprobs 等重放元数据。比较时的规范化不会修改实际发送的新 input。客户端显式提供 previous_response_id 的既有逻辑保持独立。

Codex 原生 input 中的 `additional_tools` 与 `configuration_update` 也完整参与比较。此前白名单遗漏了这两种类型，导致真实客户端请求在比较前缀前便返回 `non_self_contained_input`，无法建立检查点。工具声明或配置变化仍会回到全量。

原生 remote compaction v2 的 `compaction_summary` 必须含非空 `encrypted_content`，整项参与比较（包括 ID 和未知扩展字段），不能删除摘要或忽略密文变化。`compaction_trigger` 明确标记上下文切换：本轮不推断增量锚点、不按普通追加回合建立检查点，原因记为 `compaction_boundary`。压缩后的新输入须先完整完成一次 WS，再为后续回合建立增量检查点。其他未知条目、未解析引用继续安全采用全量。

## 大包提前选择 HTTP

HTTP→WS 路径先在取得连接租约后尝试增量，再按准备发送的未压缩 JSON 大小决定是否走 HTTP。检查包含 Codex 出站字段补齐和出口时区处理，发生在预热及业务写入之前。历史很大但新增内容小时仍可继续 WS，不以原始 HTTP 请求大小直接否决增量。

复用 `gateway.openai_ws.http_bridge_enabled` 和 `http_bridge_threshold_bytes`，默认启用、阈值 15 MiB。2026-11-03 在指定 OAuth 账号与实际出口实测：16,710,738 字节（15.9366 MiB）成功，16,837,982 字节（16.0580 MiB）收到上游 1009；两侧各复测一次，结果一致。被拒包经协商压缩后只有 12,669,657 字节（12.0827 MiB），因此保留按未压缩大小检查及 15 MiB 的余量。这是该样本的实际边界证据，不是所有端点和账号的容量承诺。

达到阈值且客户端提供完整历史时，直接发送完整 HTTP 请求，保留图片、工具、模型和推理档位；不先等待上游 1009 拒绝，也不为该请求做 WS 重试。客户端显式 `previous_response_id` 且没有完整历史时，不强行回退 HTTP。

本地大小检查不会关闭尚未写入的健康连接，也不会把整个会话永久禁用 WS。路由原因记作 `ws_session_fallback:payload_too_large`。这条路径可能已经建立或租用了 WS 连接，但不会先向上游发送超限业务包。

HTTP 成功不会把该回合补入原 WS 的检查点。因此一轮新增图片超限后，即使接下来的新消息很短，待补发给 WS 的历史仍可能超限，继续完整 HTTP 回放；真实连续两轮已复现此行为。不能把“没有永久禁用 WS”解释成“下一轮必然恢复 WS”。网关不擅自删除图片或触发有损摘要；客户端正常压缩或切换上下文后，重新比较并建立检查点。

## 连接、并发与恢复

检查点直接属于连接，只含 SHA-256 摘要、条目数、响应标识与过期时间，不保存问答正文，不写数据库或 Redis。连接数量仍由现有账号连接池与并发限制管理。自动续传成功后的 session→connection 关联最多保留 10 分钟；原有其他用途的 response/account 索引 TTL 不做全局修改。

取得租约后才判断增量，因此不会把某连接的增量发送到另一连接。连接繁忙而调度到其他连接时发送全量；同一会话的并发分支也须重新比较历史。任何上游写操作（包括原生 WS、预热）都会清除旧检查点；只有成功 completed 且下游仍连接时才建立下一检查点。

自动补入的响应锚点失效时，未输出给客户端的请求可在现有重试预算内重新发送原始完整上下文，工具调用与工具结果均保留。WS 不支持等允许 HTTP 回退的条件，同样使用完整请求。已经开始向客户端输出时不自动重放，避免重复执行或重复回答。

这项改动作用于已有 HTTP Responses→WS 路径，不将所有端点强制改为 WS；Chat Completions 兼容端点、compact、显式禁用 WS、透传等保留各自传输规则。它优化传输与续传，不保证上游 prompt cache 命中率或模型能力。

## 可观察字段

现有 `http request completed` 日志增加 `openai_ws_auto_incremental` 和 `openai_ws_auto_incremental_reason`。字段反映本请求最终 WS 尝试；退回 HTTP 时为 false/http_fallback。无问答正文、缓存键原文或密文日志。

## 验证（2026-11-03）

- 本地真实 WebSocket 测试服务器覆盖连续三轮、JSON/SSE、function/custom tool 续传、旧响应失效、HTTP 全量回退、连接关闭、过期、API key/线程隔离及并发分支。
- 新增固定 CLI 0.166.0 原生格式的多轮测试，覆盖工具声明/配置、历史图片、工具续传、压缩换窗、参数变化、增量后大小检查、HTTP 完整回放和健康连接保留。
- 最终相关 service、handler、middleware 回归 894 项测试及子用例通过，0 失败、0 跳过；包含默认 15 MiB 阈值、预热前拦截、HTTP 后连续全量回放及压缩后检查点重建。
- 测试命令：`go test -json -tags unit ./internal/service ./internal/handler ./internal/server/middleware -run 'Test.*(WS|WebSocket|HTTPToWS|Codex.*Replay|RequestAccessLogger)' -count=1 -timeout=240s`。
- 使用目标账号进行隔离真实上游四轮试验：首轮 WS 发送原生工具声明、配置和一张测试图；第二轮仅发送工具结果，第三轮仅发送新消息。两次续传均 `prefix_matched`、同一上游连接，历史图片重发 0 次；三轮均 completed。
- 第四轮在隔离进程临时降低阈值至 1 KiB，以小请求验证真实 HTTP 分支：WS 业务写入 0 次，完整 HTTP 写入 1 次且 completed；模型/档位始终为 gpt-6-astra/xhigh。该试验验证分支，不声称实测了上游 15 MiB 限制。
- 随后完成真实图片的大包边界测试：约 1、8、14.94、15.94 MiB WS 均成功；16.06 MiB 收到上游 1009 后 HTTP 成功。15.94/16.06 MiB 两侧各独立复测一次，结果相同。实际开启 15 MiB 防护后，16.06 MiB 新增图片及后两轮小消息均在 WS 写入前走 HTTP，三轮全部完成。
- 原生 v2 压缩按 CLI 形态使用 `/responses` 的 `compaction_trigger`，超限包经 HTTP 成功返回真实 `compaction`。保留该压缩结果并在其后声明当前配置，随后两轮 WS 均完成，第二轮 `prefix_matched`，实际发送 1,245 字节、仅一条新增消息；协商压缩后 103 字节。两轮均保留测试中的记忆信息（只记录判断布尔值，不保存答案）。未将旧 `/responses/compact` 的 404 或错误测试条目顺序产生的拒绝计入通过结果。
- 加密 `compaction_summary` 的整项比较、密文/ID/扩展变化以及触发边界有单元和本地转发回归；上述真实上游返回的条目类型是 `compaction`，不将其冒称为 `compaction_summary` 的线上验证。
- 隔离进程无数据库写入、调度或刷新依赖；仅保留大小、条目类型、响应 ID 哈希等元数据，未保存问答正文或图片。测试程序已从服务器清理，生产服务未更新。部署后仍需观察真实桌面长会话的命中率，不能把这四轮试验称作所有客户端/场景完整验收。
