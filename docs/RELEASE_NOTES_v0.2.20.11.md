# Sub2API 0.2.20.11

修复 Codex OAuth 请求中的合法摘要投递选项被网关整体删除的问题。

- HTTP、HTTP 转 WS、原生 WS 以及 Cursor Responses 形状桥接保留客户端明确提供的 `stream_options.reasoning_summary_delivery=sequential_cutoff`。
- 继续过滤 Chat Completions 专用的 `include_usage`、未经验证的选项及 compact 请求不适用的流式选项；API Key 路径保持原有兼容行为。
- 不主动注入摘要偏好，不改变请求模型、推理档位、上下文、工具调用关系或首字统计口径。

验证：118 项相关后端测试通过（含子项共 237 个用例），覆盖转换后的 WS 帧、HTTP/WS 规范化、compact 及 API Key 隔离。固定 Codex 0.164.0 出口身份的 HTTP、WS 兼容测试和同题 WS 对照共 10 次真实上游请求，均收到 `response.completed`。

本修复用于保留客户端已请求的协议字段。短题小样本对照不能证明长会话稳定提速，也不保证每轮生成摘要或消除所有首字等待。

仅发布 Linux amd64 二进制及嵌入式前端。运行包按明确白名单生成，不含测试、夹具、缓存或验证脚本。
