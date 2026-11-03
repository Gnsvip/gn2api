# GN2API 0.2.44

修复 Codex 原生历史在 HTTP→WS 自动增量比较中的兼容差异，并为线上未命中增加不含正文的定位信息。

## HTTP Responses 自动 WS 增量

- 补齐 agent message、local shell、tool search、web search、image generation 和 context compaction 历史条目识别；内容不完整或未知的条目仍发送完整历史。
- 规范化协议中可空字段、reasoning 重放内容及已知内部展示元数据，同时保留正文、密文、工具参数和执行元数据的严格比较。
- 补齐 local shell 与客户端 tool search 的工具结果配对检查。缺少、重复、孤立、错类型或错调用编号时不发送增量。
- 为未命中记录首个差异的条目位置和固定类型，不记录正文、图片、工具参数或密文。

## 验证与附件

- 相关 service、handler、middleware 回归共 1,061 项测试及子用例通过，0 失败、0 跳过。
- 本地真实 WebSocket 转发测试确认兼容历史可以只发送新增项，正文修改则安全发送完整历史；模型和 `xhigh` 保持不变。
- 线上 0 命中还包含没有检查点、HTTP 回退和上下文压缩边界等其他原因；部署后需结合新增诊断观察实际流量，不把本地测试当作线上命中证明。
- 仅发布 Linux amd64 二进制包、构建信息与 SHA-256 校验文件；复用已校验且未变化的前端资源，不构建容器或其他平台产物。
