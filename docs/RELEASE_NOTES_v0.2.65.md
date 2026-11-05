# GN2API v0.2.65

修复 Responses 余额或固定 Key 额度耗尽时，官方 SDK 仍自动重复请求的问题。

- HTTP 拒绝响应增加 `x-should-retry: false`，浏览器 CORS 同时暴露该响应头。保留标准 `429 + insufficient_quota`，使 Codex 和官方 SDK 能识别本轮已终止。
- 已连接 WebSocket 的额度耗尽错误携带同一标记；已经开始的 SSE 继续发送 `response.failed`，让客户端收到明确错误。
- 用户主动再次发送仍会收到余额不足提示。真正 RPM、时间窗及上游限流继续使用原有重试规则。

本次不改变余额准入、预留及结算、账号调度、首字统计或前端界面。

## 验证

- 159 个后端测试及子测试通过，覆盖 HTTP、WS、compact、已开始 SSE、CORS、固定 Key 额度及真实限流对照。
- 官方 Node SDK 7.51.0 / Python SDK 3.46.0 使用默认重试设置：同一客户端两次主动调用，新响应共 2 次请求；旧响应对照共 6 次请求。流式与非流式均验证。
- 本机 Codex CLI 0.166.0-alpha.6 通过真实网关入口测试；持续运行的 app-server 在 HTTP、WS 握手拒绝和已连接 WS 拒绝三个场景中，每轮只报告一次终止错误，随后空闲 60 秒无自动重试。
- 发布产物为 Linux amd64 二进制，内嵌从同一提交构建的前端，附构建信息与 SHA256 校验文件。

自行忽略错误并再次发起新请求的第三方外层循环，不能由该协议标记强制终止。详见[余额拒绝处理说明](../deploy/balance-denial-repair.md)。
