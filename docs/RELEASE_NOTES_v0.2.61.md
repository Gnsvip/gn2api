# GN2API v0.2.61

为提前发送处理通知、随后集中返回内容的 OpenAI API Key 上游增加可撤销的账号级 HTTP 首次响应计时例外。默认关闭；管理员可通过账号 `extra.openai_http_ttft_mode = "first_event"` 显式启用，无需改动全局计时设置。

- 普通 HTTP 流式 Responses（含 Responses 转 Chat 输出）记录收到第一个有效、非终止 Responses 事件的实际耗时，包含 `response.created` 和 `response.in_progress`。该时间表示上游已响应，不表示已经开始推理或出字。
- 错误、额度通知、心跳和单独终止事件不填首字；不固定耗时，不修改历史记录。
- 没有配置例外的账号保持原有计时。WS 入口、WS 出口及 WS 转 HTTP 桥接仍使用 `ws_semantic`。
- 首内容、首正文、写出和 Flush 诊断继续独立记录。账号性能观察按计时模式分组，不将首次响应与 semantic 混算。
- 计时例外不改变输出超时、响应提交、缓冲和失败切号。删除账号配置即可恢复默认 HTTP 计时，不必再次部署。

## 验证

23 项顶层测试通过，共 146 个通过项（含子场景），无失败或跳过。覆盖账号显式启用及关闭、其他账号/平台隔离、普通 HTTP/透传/Chat 输出、WS 桥接隔离、错误切号、首输出超时和性能观察分组。测试使用本地合成事件，不消耗真实模型 token。

仅发布 Linux amd64 二进制，内含同源构建的前端，并附 SHA-256 校验和构建记录。前端和 ModelTrace 扩展没有功能变更，不构建其他平台或容器镜像。Fast 放行属于独立的运行配置，本版本不修改它。

计时定义与撤销方式见[请求耗时说明](../deploy/request-timing-evidence.md)。
