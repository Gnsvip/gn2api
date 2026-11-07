# HTTP Responses 自动 WS 增量续传

普通客户端继续 POST `/v1/responses`。OAuth 账号已启用的 HTTP→WS 路径，在同一上游连接仍有效且历史相符时，自动发送 `previous_response_id` 与新增 input；客户端无需开启 `supports_websockets`。客户端与网关之间仍是原来的 HTTP/SSE。

## 采用增量的条件

- 已取得实际连接的独占租约，该连接上一轮成功完成，检查点未超过 10 分钟。
- 分组、API key、原始线程/会话执行作用域一致。
- 最终出站模型、推理等级、工具定义及其他非传输参数一致，上下文窗口未变化。
- 本轮完整历史的前缀等于上一轮完整输入加上游输出，且存在新增 input。
- 历史可自行还原，上一响应的全部 function/custom tool、local shell 和客户端 tool search 调用在新增输入中都有类型和 call_id 相符的真实结果。服务端 tool search 不要求客户端提交结果。缺项、重复、孤立结果或新工具调用未闭合时采用全量，不编造结果。
- 上游输出可由 `response.output_item.done` 与 `response.completed.output` 确认完整。末帧缺项可用流中已完成的项补齐检查点；未解析的 item_reference、未知类型、输出索引缺洞或冲突时采用全量。

比较保留消息正文、密文、工具名称/参数及 call_id；已有 message/reasoning/function/custom 等条目忽略输出对象 ID、status，新增原生类型保留 ID/status。忽略已知内部 turn_id/create_time/content_item_kinds/content_item_metadata 和空 annotations/logprobs 等重放元数据；工具执行元数据和未知字段继续严格比较。比较时的规范化不会修改实际发送的新 input。客户端显式提供 previous_response_id 的既有逻辑保持独立。

原生回放还会省略输出的 `metadata.turn_id`、`internal_chat_message_metadata_passthrough.content_token_count` 及空元数据包。摘要比较将内部元数据包的 null、空对象和省略视为等价。v0.2.47 修复了空元数据引起的误判；v0.2.48 进一步兼容官方客户端在反序列化工具输出时省略非空 `cell_id`、`executed_tool_calls`、`tool_calls_complete` 的行为：只为上一轮上游 function/custom tool 输出保留原始摘要和精确省略这三项的替代摘要。已作为输入发送的历史继续严格比较，下一检查点以实际输入重新计算；工具名称、参数、调用关系、密文及未知字段变化仍阻止增量。这不是对所有历史统一删除执行元数据。

Codex 原生 input 中的 `additional_tools` 与 `configuration_update` 也完整参与比较。此前白名单遗漏了这两种类型，导致真实客户端请求在比较前缀前便返回 `non_self_contained_input`，无法建立检查点。工具声明或配置变化仍会回到全量。

原生 remote compaction v2 的 `compaction_summary` 必须含非空 `encrypted_content`，整项参与比较（包括 ID 和未知扩展字段），不能删除摘要或忽略密文变化。`compaction_trigger` 明确标记上下文切换：本轮不推断增量锚点、不按普通追加回合建立检查点，原因记为 `compaction_boundary`。压缩后的新输入须先完整完成一次 WS，再为后续回合建立增量检查点。其他未知条目、未解析引用继续安全采用全量。

## 大包提前选择 HTTP

HTTP→WS 路径先在取得连接租约后尝试增量，再按准备发送的未压缩 JSON 大小决定是否走 HTTP。检查包含 Codex 出站字段补齐和出口时区处理，发生在预热及业务写入之前。历史很大但新增内容小时仍可继续 WS，不以原始 HTTP 请求大小直接否决增量。

复用 `gateway.openai_ws.http_bridge_enabled` 和 `http_bridge_threshold_bytes`，默认启用、阈值 15 MiB。2026-11-03 在指定 OAuth 账号与实际出口实测：16,710,738 字节（15.9366 MiB）成功，16,837,982 字节（16.0580 MiB）收到上游 1009；两侧各复测一次，结果一致。被拒包经协商压缩后只有 12,669,657 字节（12.0827 MiB），因此保留按未压缩大小检查及 15 MiB 的余量。这是该样本的实际边界证据，不是所有端点和账号的容量承诺。

达到阈值且客户端提供完整历史时，先检查能否在当前新连接分段恢复（见下节）；不符合恢复条件时发送完整 HTTP 请求，保留图片、工具、模型和推理档位，不先等待上游 1009 拒绝。客户端显式 `previous_response_id` 且没有完整历史时，不强行回退 HTTP。

本地大小检查不会关闭尚未写入的健康连接，也不会把整个会话永久禁用 WS。路由原因记作 `ws_session_fallback:payload_too_large`。这条路径可能已经建立或租用了 WS 连接，但不会先向上游发送超限业务包。

HTTP 成功不会把该回合补入原 WS 的检查点。因此一轮新增图片超限后，即使接下来的新消息很短，待补发给 WS 的历史仍可能超限，继续完整 HTTP 回放；真实连续两轮已复现此行为。不能把“没有永久禁用 WS”解释成“下一轮必然恢复 WS”。网关不擅自删除图片或触发有损摘要；客户端正常压缩或切换上下文后，重新比较并建立检查点。

### 新连接恢复超过单条消息阈值的历史

部署或重连丢失连接检查点后，完整历史超过 15 MiB 曾导致每轮持续回退 HTTP，直到客户端压缩。现在 HTTP→OAuth WS 的首个尝试，在没有旧锚点且完整历史符合既有校验时，按条目分段发送：前几段使用 `generate:false`，每段收到无输出的成功终态后，下一段使用该连接刚返回的 `previous_response_id`；最后一段才生成用户请求的回答。成功后，检查点仍覆盖原始完整历史，后续工具结果按原有严格前缀比较采用增量。

分段保留每个条目的内容、顺序和全部模型/推理/工具参数；工具调用及结果保持同段，推理条目与后续条目一起发送。整包先按出口时区归一化，单段仍须小于现有阈值。恢复上限为 64 MiB、8 段及 45 秒，单条或完整工具组超限、压缩触发请求、未知/不完整历史和显式外部锚点不采用此恢复。没有保存用户正文或跨连接复用旧响应 ID。

内部预热事件不发送给用户，也不提前触发首字；首字仍包含恢复所花的真实时间。预热返回的实际用量独立预留和结算（请求标识 `ws_history_restore:`），包括后续恢复失败的情况，不能假定 `generate:false` 免费；内部记录不填首字且不标记为上下文压缩。父请求取消后停止恢复。恢复失败的连接丢弃，常规大包兜底仍使用完整原请求；已识别的凭证、限流和上下文恢复错误交回既有错误处理。

15 MiB 是本项目基于上述 OAuth 样本设置的安全余量，不是官方公开文档公布的统一限制。[官方 WebSocket 指南](https://developers.openai.com/api/docs/guides/websocket-mode)描述了 `generate:false` 预热、响应锚点续传和断线后重新发送完整上下文。公开 API 的说明不替代 Codex OAuth 后端验收。

2026-11-07 隔离验收：实际 Forward 代码使用账号 #12275 与已核对出口，16,792,050 字节合成图片请求在默认阈值下拆成 12,595,162 字节预热和 4,199,223 字节生成，两段均走 WS；随后两轮成功 `prefix_matched` 并完成工具续传。另有三段小型真实预热验证不同历史标记均可引用。217 项本地测试及子用例通过，覆盖连接池重建、完整历史与图片、工具分组、大小边界、取消、失败回放、凭证/密文错误及预热用量。未更新生产服务，不代表原始 #455 聊天已在部署后验收。

## 连接、并发与恢复

检查点直接属于连接，只含 SHA-256 摘要、条目数、响应标识与过期时间，不保存问答正文，不写数据库或 Redis。连接数量仍由现有账号连接池与并发限制管理。自动续传成功后的 session→connection 关联最多保留 10 分钟；原有其他用途的 response/account 索引 TTL 不做全局修改。

工具续传额外保留调用编号的 SHA-256 摘要与预期结果类型；流中临时收集的单轮输出指纹最多 2,048 项。长历史修复将完整输入加输出的检查点上限独立调整为 32,768 项；超过时仍完整转发，不截断或丢弃上下文。这是网关的资源保护值，不是 OpenAI 的 token 或协议上限。

检查点的滚动 SHA-256 前缀仍覆盖全部历史，只保留末尾最多 2,048 项的逐条指纹，且切片容量同样有界；在 amd64 上该数组最多 180,224 字节（176 KiB），另有最多 64 项字段摘要和最多 2,048 项待完成工具关联等开销，不代表整个请求的峰值内存。较早历史的正文、图片、密文或工具结果变化仍会导致完整前缀不一致。逐条指纹尾部覆盖上一轮全部输出，因此原生工具输出的已验证回放兼容保持有效；早于尾部的差异可能只有 `prefix_mismatch`，不再保证能定位精确条目或字段。不会持久保存工具内容、图片或推理密文，也不修改客户端收到的流事件。

v0.2.48 为前缀不一致诊断额外保留最多 64 项上游输出的固定字段摘要，随连接检查点替换及过期，不累计保存各轮历史。日志中的 `openai_ws_auto_incremental_changed_fields` 只来自固定字段枚举，不输出字段值、摘要或客户端自定义字段名称。

v0.2.49 统一官方客户端无 base-instructions 标签时的原生识别，保留工具 `call_id`，避免网关自身将 `call_` 改写为 `fc_` / `ctc_` 而造成前缀不匹配。消息及错误类型的工具条目 `id` 过滤保持独立；增量比较仍严格核对工具关联和参数。

HTTP → WS 的已有访问记录增加最后一次尝试的数字诊断：`ops_openai_ws_attempt`、`ops_openai_ws_attempt_start_ms`、`ops_openai_ws_acquire_ms`、`ops_openai_ws_queue_wait_ms`、`ops_openai_ws_conn_pick_ms`、`ops_openai_ws_write_ms`、`ops_openai_ws_request_sent_ms`、`ops_openai_ws_first_event_ms`、`ops_openai_ws_first_visible_ms`，以及布尔字段 `ops_openai_ws_conn_reused`。start/sent/first_event/first_visible 是相对 Forward 内现有 startTime 的偏移，其余 ms 字段是本次尝试的阶段耗时；queue/pick 与 acquire 可能重叠，不应相加。发送后至可见内容的间隔包含上游和网络等等待，不等于纯模型推理时间或客户端渲染时间。重试清除未到达阶段的旧值；原生 WS 尚未增加同等逐轮分段记录。

取得租约后才判断增量，因此不会把某连接的增量发送到另一连接。连接繁忙而调度到其他连接时发送全量；同一会话的并发分支也须重新比较历史。任何上游写操作（包括原生 WS、预热）都会清除旧检查点；只有成功 completed 且下游仍连接时才建立下一检查点。

自动补入的响应锚点失效时，未输出给客户端的请求可在现有重试预算内重新发送原始完整上下文，工具调用与工具结果均保留。WS 不支持等允许 HTTP 回退的条件，同样使用完整请求。已经开始向客户端输出时不自动重放，避免重复执行或重复回答。

自动增量收到 `No tool output found for function call ...` 或 `custom tool call ...` 时，只有完整原请求确实包含被拒调用及配对结果、其他工具链也完整且客户端仍连接，才丢弃错误连接并恢复完整请求。恢复后不再次套用旧增量锚点；若完整请求仍报同样错误，直接返回错误。恢复全程保留 `include`（包括 `reasoning.encrypted_content`），模型、推理等级、工具和历史仍按原请求发送。

这项改动作用于已有 HTTP Responses→WS 路径，不将所有端点强制改为 WS；Chat Completions 兼容端点、compact、显式禁用 WS、透传等保留各自传输规则。它优化传输与续传，不保证上游 prompt cache 命中率或模型能力。

## 临时故障后的会话恢复

网关不再因一次 `dial_failed` 等临时错误将活跃会话长期固定在 HTTP。首次失败冷却 30 秒；恢复失败依次延长为 60、120、240、300 秒，之后上限为 5 分钟。冷却从失败计时，成功 HTTP 请求只更新空闲淘汰时间，不推迟恢复。`upgrade_required` / `ws_unsupported` 等能力信号仍保留会话禁用；401、策略拒绝、限流和单次 403 维持各自已有处理。

冷却结束后，同一凭据归属、账号、分组、API Key 和客户端执行会话只放行一个恢复请求。请求必须没有显式 `previous_response_id`、没有纯 item 引用，且工具结果具备配对调用；否则不拿增量内容建立新根。恢复强制新建上游 WS，清理该执行会话的旧连接/turn-state 关联，完整发送当前历史。只有成功终态完成后才解除回退；新连接的检查点继续按现有规则完整校验，后续工具结果才能自动增量。

HTTP→WS 的获取连接、握手与普通重试共用原有总重试预算；该预算仅作为获取连接的截止时间传递，不给正常推理/生成设置同样的总超时。原有单次凭据任务、失效锚点或密文修复仍分别保留其有界重试机会。恢复请求失败或取消会释放探测资格；已输出的响应不回放。原生 WS 的上下文池和透传入口也使用同一恢复准入规则。

新增 `session_recovery_probe`、`session_recovery_succeeded`、`session_recovery_deferred` 日志只包含账号 ID、错误分类、次数和冷却毫秒数，不保存正文、图片、密文或完整请求头。恢复策略用于减少连接故障的额外等待，不保证上游内容生成时间或首字小于 10 秒。

## 模型目录与压缩设置

2026-11-03 的只读官方 Codex `client_version=0.166.0` 清单中，Astra 的默认/最大窗口是 272000 / 872000；公共 API 的 1050000 不能直接作为 OAuth Codex 默认窗口。OAuth Astra 缺少已同步窗口时使用上述 Codex 回退值，账号明确同步的窗口仍优先，API Key 默认不受此修改影响。2026-11-04 只读检查确认账号 #12249、#12250 没有保存模型元数据，因此会走回退分支。

目录同步及生成补齐 `auto_compact_token_limit` 和 `effective_context_window_percent`。上游明确的有效数值随快照保留；未声明/null 保留模型默认，不推算或强制统一提前压缩阈值。实时清单的 null 不被旧快照的阈值覆盖。多个路由候选必须都明确声明相应设置才生成公共覆盖值，采用其中较小值。

配置参考：[官方 Codex 配置说明](https://learn.chatgpt.com/docs/config-file/config-reference)、[网关模型目录说明](https://learn.chatgpt.com/docs/model-catalogs)。客户端实际是否读取新目录、是否存在本地覆盖以及当前会话已缓存的窗口，仍应以客户端日志核验。此修改不主动压缩、删减历史或改动推理等级；先前本机原生压缩实测不等于本次变更已在生产验收。

本次最终验证命令：`go test -tags=unit ./internal/service ./internal/handler ./internal/server/middleware ./internal/pkg/requesttiming -run 'Test.*(WS|WebSocket|HTTPToWS|Codex|UpstreamModel|ExtractUpstream|RequestTiming|RequestAccessLogger|Logger_|OpenAIEnsureForwardErrorResponse|OpenAIForwardErrorAlreadyCommunicated)' -count=1 -timeout=240s -json`。2,526 项测试及子用例通过，0 失败；1 项 `TestScopedCodexWriteAuthorizationAndTransactions` 因未配置独立 `SCOPED_ADMIN_TEST_DSN` 跳过。

新增本地网络验收覆盖 HTTP 回退→新 WS 完整历史→下一轮工具增量，以及原生 WS 上下文池/透传的恢复与工具续传。另验证单探测并发准入、冷却不被 HTTP 延长、能力拒绝保持禁用、过期探测不能清除新状态、慢握手受连接预算约束、正常生成可超过连接预算，以及目录同步和实时 null 的优先级。测试没有向真实上游发起新生成；前端、客户端配置和生产服务未修改，尚未发布部署。

## 可观察字段

现有 `http request completed` 日志增加 `openai_ws_auto_incremental` 和 `openai_ws_auto_incremental_reason`。字段反映本请求最终 WS 尝试；退回 HTTP 时为 false/http_fallback。无问答正文、缓存键原文或密文日志。

工具续传修复增加 `openai_ws_auto_incremental_succeeded`（增量收到成功 completed，且正常完成下游交付）和 `openai_ws_auto_incremental_replay`（本请求触发过自动锚点恢复）。不能把 `auto_incremental=true` 或 SSE 的 HTTP 200 单独视为增量成功；完整回放成功也不计作增量成功。

原生条目兼容修复增加 `openai_ws_auto_incremental_item_index`（从零开始）、`openai_ws_auto_incremental_item_type` 和 `openai_ws_auto_incremental_previous_item_type`，定位不支持的条目或留存逐条指纹中的第一个不匹配处。长历史早于指纹尾部的部分仍严格比较，但无法靠这些诊断排除更早的差异。类型仅从固定枚举输出，未知类型一律写 `unknown`；不输出正文、原始类型字符串、ID、摘要、工具参数、图片或密文。没有条目差异时省略这些字段，每次 WS 尝试清理旧诊断。

## v0.2.50 长历史增量修复验证（2026-11-04）

- 修改前，本地完整 Forward → WS 测试以 4,096 条历史复现 function/custom 两类工具续传均被 `history_too_large_for_checkpoint` 阻止。
- 修改后，同一测试连续完成五轮：首轮全量、两轮增量、主动关闭连接后完整恢复、新连接再次增量。模型、推理档位和工具关联保持一致，增量不重复发送历史图片；最大单轮输出的 2,048 项回放替代摘要全部保留。
- 2,047、2,048、2,049、4,096 及接近 32,768 项的边界通过；amd64 的逐条指纹数组容量均不超过 176 KiB。窗口外早期图片、密文和工具结果变化仍阻止增量；压缩换窗先全量建立新检查点，再恢复增量。32,768 项总上限和 2,048 项单轮输出上限均保留超限回退测试。
- 相关 service、handler、middleware、openai_ws_v2 共 576 个顶层测试（含子用例 1,157 项）通过，0 失败、0 跳过。
- 使用本机 `codex-cli 0.166.0-alpha.5.1`、账号 #12248、`gpt-6.1-sol/low`，在独立测试进程中运行当前修改后的实际 Forward 代码。每轮向原生历史插入同一组 4,096 条合成消息，并附带一张合成图片。真实上游首轮收到 4,104 项、536,405 字节；后两轮均 `prefix_matched` 且成功完成，只发送一项 custom tool 结果，分别为 2,241 / 2,243 字节，历史图片数均为 0，三轮使用同一条 WS。
- 合成填充由隔离网关插入，不声称原生客户端自行积累了 4,096 条历史，也不将其作为全部生产长会话已修复的证明。未修改客户端配置，未更新或重启生产服务；只保存条目数、大小、状态、连接关联等元数据，测试进程及服务器临时文件已清理。

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

## 工具续传修复验证（v0.2.43）

- 修复前用本地 WebSocket 服务复现：流事件已经发出 function/custom tool 调用，终态 `output` 为空时，下一轮错误地发送两项增量（旧调用＋结果），预期应仅发送一项真实工具结果。两类工具的失败用例在修复后通过。
- 新增覆盖末帧空/缺失/部分输出、乱序与重复 done、冲突和缺洞、2048 项边界、并行工具结果配对，以及 `error` / `response.failed` 两种拒绝形式。
- 本地真实 WebSocket 转发测试确认：缺工具结果拒绝可换连接完整回放；已向客户端输出、原请求缺结果、无关调用错误均不进行该恢复；完整回放再次报错不会循环。回放保留推理密文的 `include` 声明。
- 相关 service、handler、middleware unit 回归共 953 项测试及子用例通过，0 失败、0 跳过。命令：`go test -json -tags unit ./internal/service ./internal/handler ./internal/server/middleware -run 'Test.*(WS|WebSocket|HTTPToWS|Codex.*Replay|RequestAccessLogger|Logger_)' -count=1 -timeout=240s`。
- 以上是本地受控上游与回归结果。验证阶段未更新生产，未使用账号发起新的真实上游验收；不能据此断言此前全部线上工具错误均由同一原因产生，也不能据此承诺首字时间。

## 握手 403 与重复错误响应修复（v0.2.43）

生产排查发现 WS 握手 403 被和 401 一起归为 `auth_failed`，导致已有的 `upstream_forbidden` HTTP 回退分支无法命中。修复将无结构化账号/权限拒绝信号的握手 403，以及已识别的 Cloudflare 拦截响应，归入 `upstream_forbidden`。不重复尝试 WS，在尚未向客户端输出、请求未取消、没有显式续传锚点且原请求可完整回放时，按已有 HTTP→WS 桥接规则回退到 HTTP。

401、明确的凭据失效/账号状态错误、认证挑战和结构化权限/策略拒绝不使用这条回退。单次握手拦截也不会永久禁用会话 WS 或直接封停账号；若 HTTP 同样拒绝，继续进入既有 HTTP 错误处理，不能假装成功。

最终握手错误通过 JSON 返回后标记响应已交付，handler 不再追加通用 SSE `response.failed`。客户端收到一份完整错误，保留握手阶段和状态信息。

本地真实握手测试覆盖 service 和完整 Responses handler：流式/非流式 403→HTTP 成功、HTTP 也拒绝、401、结构化凭据/权限 403、显式锚点和已写下游。回放断言保留模型、`xhigh`、完整工具调用/结果及 `reasoning.encrypted_content`，不依靠孤立的分类器单元测试代替转发验证。测试仅重定向本地网络目的地，未请求生产账号或修改线上配置。

最终相关回归共 1,007 项测试及子用例通过，0 失败、0 跳过，包含前述工具增量修复。命令：`go test -json -tags unit ./internal/service ./internal/handler ./internal/server/middleware -run 'Test.*(WS|WebSocket|HTTPToWS|Codex.*Replay|RequestAccessLogger|Logger_|OpenAIEnsureForwardErrorResponse|OpenAIForwardErrorAlreadyCommunicated)' -count=1 -timeout=240s`。线上效果须发布部署后核对，HTTP 回退成功不能当成消除了上游握手拒绝的根因。

## 原生历史兼容修复（本地验证，尚未部署）

v0.2.43 上线后于 2026-11-03 12:16:39 核对的生产样本中，493 次自动增量评估全部未命中：221 次 `non_self_contained_input`、139 次 `prefix_mismatch`、95 次 `no_checkpoint`、37 次 `http_fallback`、1 次 `compaction_boundary`。旧日志没有条目类型和差异位置，因此不能据此认定下面的兼容问题解释了全部线上未命中。

对照 [Codex CLI 0.166.0 的原生协议类型](https://github.com/openai/codex/blob/rust-v0.166.0/codex-rs/protocol/src/models.rs)，本地先复现并修正：

- 白名单遗漏 `agent_message`、`local_shell_call`、`tool_search_call`、`tool_search_output`、`web_search_call`、`image_generation_call` 和 `context_compaction`。新增类型须有可用内容，不能仅靠 ID 视为完整；未知类型继续全量。
- 协议规定的可空字段在重放时可能省略；reasoning 的空 content 与省略等价。只规范化该类型已知可空字段，实际值、未知扩展、工具执行元数据变化仍阻止增量。
- 网关现有逻辑会在下一轮转发前删除非便携 reasoning.content。检查点比较在存在非空 encrypted_content 时采用相同投影，继续严格比较密文和 summary；没有密文保护时不忽略非空 content。
- 工具配对覆盖 local shell 的 function_call_output 以及客户端 tool search 的 tool_search_output，缺结果、错 call_id、错结果类型、重复和孤立结果保持全量。

本地真实 WebSocket 服务配合生产 Forward 路径验证：原生历史、内部标记及 reasoning.content 重放差异可在同一连接第二轮命中增量，只发送一条新增消息；正文改动则完整发送历史并给出差异位置，两条路径均保留模型和 xhigh。新增诊断只保留类型、位置和内存摘要，不增加问答正文持久化。

本轮尚未发布或更新生产。部署后的验收应分别核对增量尝试、成功 completed、恢复全量，并按新增诊断分析仍未命中的原因；本地受控测试通过不代表真实流量已命中，也不承诺降低上游推理时间。
