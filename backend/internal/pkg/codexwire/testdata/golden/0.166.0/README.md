# Codex CLI 0.166.0 采集样本

2026-10-31 在 WSL Ubuntu 24.04 x86_64 使用官方 Linux musl 正式版采集。采集器来自 `zuchengchen/sub2api` 的 `czc-v2026.10.30.2` 快照（提交 `4ad3bb5d7e2d0ca74b274f05dbd64c3d3c3a3571`），路径 `backend/tools/codexgolden`，运行时未修改。

官方来源：

- 发布：https://github.com/openai/codex/releases/tag/rust-v0.166.0
- 包：https://github.com/openai/codex/releases/download/rust-v0.166.0/codex-x86_64-unknown-linux-musl.tar.gz
- 包 SHA256：`a0bf572d2f93f056c6712a6da348e96078dc84f3885fd7be67807ef389cb3978`
- 官方提交：`6ff1e1ca3b30fc8b914720681fcf5fe62e95f856`
- 模型目录：https://raw.githubusercontent.com/openai/codex/6ff1e1ca3b30fc8b914720681fcf5fe62e95f856/codex-rs/models-manager/models.json

使用 `-expect-version 0.166.0`、对应提交的 `-models-json`、`-source-commit` 和默认启用的 strace 审计。隔离的 HOME/CODEX_HOME 使用虚构 OAuth 凭据；所有测试服务均为本机假上游。六个场景共 142 条记录；没有观察到非回环连接，6 次硬编码分析请求被本机拒绝代理阻止。具体版本、时间、二进制和模型目录摘要见 `meta.json`。

CLI 实际身份为 `codex_exec/0.166.0 (Ubuntu 24.4.0; x86_64)`。网关保留已有规范 CLI 身份，同时为 0.166.0 接受成对的 `codex_exec` 身份；不更改全局身份配置。

默认 TLS 样本和注入自定义 CA 后的 TLS 样本分开保存。后者会切换 TLS 实现，不可代替默认路径样本。回归检查 TLS 结构、请求头顺序、帧标记和解压后的原始 JSON；不要求握手随机值、随机掩码和压缩后字节长度完全相同。

这些样本证明本地线路行为，不证明真实账号认证、生产环境稳定性或上游服务接受所有请求。
