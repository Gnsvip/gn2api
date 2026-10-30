# Sub2API 0.2.20

## Codex egress transport (experimental)

- Adds an opt-in HTTP and WebSocket wire transport for explicitly allowlisted OpenAI OAuth accounts.
- The transport is disabled by default. Enable `gateway.codex_egress.enabled` and provide specific account IDs in `gateway.codex_egress.account_ids` before it can be selected.
- Verified Codex CLI profiles are limited to `0.163.0` and `0.164.0`, the canonical Ubuntu CLI identity, and `chatgpt.com:443` Codex Responses requests. Other requests continue through the existing transport.
- The production installation reports Codex CLI `0.166.0`; that version is not covered by the captured profiles and will continue using the existing transport.
- HTTP uses the captured TLS profile and zstd request encoding where the request body can be replayed. WebSocket uses the custom Codex TLS handshake and frame transport.

The release is based on the current `Gnsvip/gn2api` main branch and includes its existing changes. The Codex transport itself does not add a database migration or alter billing behavior.

## 本次发行与部署

- 发行仓库为 `Gnsvip/gn2api`，提供包含完整前端的 Linux amd64 程序包、`checksums.txt` 和构建来源记录；本次不发布 Docker 镜像或其他平台的二进制。
- 新出站传输属于实验功能，默认关闭，须显式配置账号白名单。只验证了 Codex CLI `0.163.0` / `0.164.0`；当前生产使用的 `0.166.0` 继续使用原传输。
- GitHub 发布和服务器部署分开执行。发布不会上传文件到生产服务器，也不会替换程序或重启服务；部署须经用户再次确认。
- 后续更新应使用本仓库的发行包。继承的安装脚本仍默认指向上游 `Wei-Shaw/sub2api`，不作为本次定制版本的升级入口。
- 测试范围及未完成的验证以 GitHub Release 中的验证说明为准；交叉编译成功不代表已通过生产环境验证。
