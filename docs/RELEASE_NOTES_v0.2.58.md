# GN2API v0.2.58

修复原生 WS 后续轮次换号时无法携带完整上下文的问题，并新增“首次响应”统计口径。

- `ctx_pool` 后续轮次在尚未输出本轮内容时遇到可换号错误，重试失败的当前轮，保留模型、推理档位、指令、历史助手回答和工具调用关系。支持切换至明确配置为 `http_bridge` 的 Responses 账号，并继续后续轮次。
- HTTP 桥接补齐普通助手回答的回放与输出完整性校验。未知旧响应锚点、缺失历史、错误响应链、旧账号 `item_reference` 等不完整上下文不会被静默丢弃。
- 新增 `first_event` 口径：从开始转发到收到首个有效上游 Responses 事件，包含开始处理通知。它表示首次响应，不表示文字已经到达；不会改变正文速度、首次输出超时或失败重试边界。保留 `visible`、`semantic`，默认仍为 `visible`，历史记录不重算。
- 首次内容与正文到达、写出时间仍分别记录为诊断元数据，不新增正文日志。

HTTP 备用账号需开启全局 `gateway.openai_ws.mode_router_v2_enabled`，并将账号模式设为 `http_bridge`。`off` 仍保持关闭含义。主备选择沿用分组、模型、账号状态、优先级与并发配置。详见 [WS 换号与 HTTP 备用说明](WS_HTTP_ACCOUNT_FAILOVER.md)。独立 `passthrough` 模式后续轮次仍使用原有重连策略。

验证包含受控真实 WS 握手 401、限流换号、HTTP 备用工具续传、上下文缺失保护，以及 HTTP、HTTP↔WS、原生 WS 的计时回归；前端设置与中英文键检查 48 项通过，类型检查通过。受控故障测试不代表线上已经发生同样的故障切换。

仅提供 Linux amd64 二进制，内含重新构建的前端；附校验文件与构建记录。不构建额外平台或容器镜像。

Backend release regression: 5,462 tests/subtests passed, 6 skipped under existing conditions, 0 failures across seven packages.
