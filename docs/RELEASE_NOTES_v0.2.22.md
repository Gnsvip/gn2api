# GN2API 0.2.22

## Codex 专用出站默认开启

- 全局配置 `gateway.codex_egress.enabled` 改为默认开启。未配置这一项时，符合条件的 OpenAI OAuth 账号自动使用 Codex 专用出站；现有账号和以后新增的账号都无需手工加入名单。
- 移除账号白名单限制。旧配置中的 `account_ids` 可以删除，保留也会被忽略，不再限定适用账号。旧配置若明确设置 `enabled: false`，仍然保持关闭。
- 保留 Codex CLI `0.163.0`、`0.164.0`、`0.166.0` 的适配，以及账号类型、版本、出站身份、目标地址校验。未知版本、身份不匹配和其他上游地址继续使用原有传输方式。
- HTTP 与 WebSocket 继续按原有协议配置选择；默认启用专用出站不会强制所有请求改为 WebSocket。HTTP 使用对应 TLS 模板及可重放请求体的 zstd 编码，WebSocket 使用对应握手及帧传输。

## 如何使用

升级后，未显式关闭的实例会自动启用，无需在前端逐个操作账号。也可在现有 `gateway` 节点下明确设置：

```yaml
gateway:
  codex_egress:
    enabled: true
```

需要关闭时改为 `false` 并重启程序；环境变量 `GATEWAY_CODEX_EGRESS_ENABLED=false` 同样有效，且优先于配置文件。新配置示例已经同步更新。

适用请求的上游为 `chatgpt.com:443/backend-api/codex/responses`，使用 HTTPS 或 WSS；出站 User-Agent 和 Version 必须对应已验证版本及 Ubuntu CLI 身份。这里检查的是服务器发出的请求身份，不要求用户电脑运行 Ubuntu。账号页面原有的“Codex 指纹收敛”选项与此全局传输配置是两项独立设置。

## 发行与验证

- 本版发布到 `Gnsvip/gn2api`，提供包含前端的 Linux amd64 二进制程序包、SHA256 校验文件和构建来源记录。现有二进制安装方式可继续使用，无需改为容器。
- 相对 v0.2.21 不新增数据库迁移，不修改前端、计费、倍率、分组或其他平台业务逻辑。
- 回归覆盖默认启用、配置和环境变量关闭、旧白名单兼容，以及三版 CLI 与新增账号的 HTTP／WebSocket 选路。
- 本次只发布版本，生产服务器仍为 v0.2.21；待用户确认后再更新。

本机离线测试不等同于真实 OpenAI 账号端到端验证；本次没有连接生产数据库、Redis 或真实上游账号。具体测试与发行包启动结果见 GitHub Release 和 `build-info.json`。
