# GN2API v0.2.23

## 中文更新说明

本版本把 OpenAI OAuth 的普通 HTTP 入站请求自动接入上游 Responses WebSocket。客户端无需改成 WebSocket，也不需要管理员逐个把账号加入名单；新建 OAuth 账号默认使用 `ctx_pool`。API Key、显式关闭、紧凑端点和 HTTP passthrough 继续保持原有 HTTP 行为。

WS 能力失败时，仅在下游尚未输出且请求没有续链锚点的情况下回退到 HTTP；同一逻辑会话会记住能力失败，避免每轮重复探测。已输出部分不会重放，续链请求不会被错误地改成无状态 HTTP。

Codex 出站继续只对已验证的 OAuth 身份和版本 `0.163.0`、`0.164.0`、`0.166.0` 启用专用线路，并补齐参考采集版中缺失的行为：

- HTTP/WS 头字段投影到 Codex 的 `session-id`、`thread-id`，清理 legacy 下划线头、安装 ID 和 HTTP 上的 `openai-beta`；WS 的 `x-codex-turn-state` 放入 `client_metadata`。
- WS `response.create` 使用 Codex 的字段顺序，补齐官方恒发的 `stream`、`input`、`tool_choice`、`parallel_tool_calls`、`reasoning`、`store`、`include` 字段，并保持嵌套值。
- HTTP 请求体在 zstd 压缩前按 Codex Responses 结构顺序组织。
- 顶层 `instructions` 在 WS Codex 请求中转换成首个 developer input item，并保留 `model.base_instructions` 元数据。

管理界面的 OAuth WS 默认值同步为 `ctx_pool`；日志记录最终上游传输、选路原因及 WS 连接状态。

## 兼容性与迁移

无需数据库迁移。已有 OAuth 账号在没有显式 `off`、`http_bridge` 或强制 HTTP 设置时自动采用 WS；已有显式账号设置优先。生产升级前请确认上游账号可用，并保留上一版二进制以便回退。

## 验证

- `go test -p=2 -tags=unit ./...`：完整结果以发布包 `build-info.json` 和测试摘要为准。
- Codex golden：`0.163.0`、`0.164.0`、`0.166.0` 的 HTTP/WS 头顺序、TLS 模板、帧压缩与掩码回归。
- HTTP→WS 桥接、426 会话级回退、续链保护、partial output 不重放回归。
- 前端测试和嵌入式 Linux amd64 启动检查：结果以发布包中的验证记录为准。

## 回退

生产部署时先备份当前 `/opt/sub2api/sub2api`，若出现异常可停止新进程并恢复上一版备份后启动。v0.2.23 本次仅先发布，服务器保持当前生产版本，待确认后再部署。
