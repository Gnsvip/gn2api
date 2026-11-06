# 欠费重复请求防护

适用：有效 API Key、站内余额不足，客户端仍不断提交新请求。普通余额不足仍返回 403；Codex Responses SSE 仍返回 HTTP 200 的 `response.failed / insufficient_quota`，业务记录为 403。不会将站内余额不足改成上游 429。

## 应用行为

- 每次请求先走已有鉴权、用户/分组/订阅及余额校验。确认欠费后才进入防护，不给正常有余额、有效订阅、simple 模式或免计费查询增加等待。
- HTTP 不读取正文。拒绝的 HTTP/1 连接不为复用而继续排空历史；HTTP/2 不发送非法 Connection 头。
- 同一欠费用户（所有 Key、HTTP 和 WS 共用）每 5 分钟保留第一条完整错误，其余写 `balance_denied_repeat` 入口聚合，保留用户、Key、次数、首次/末次时间，不保存正文，也不逐条写访问日志。
- Codex 欠费 WS 每个用户每 5 分钟最多提供 3 次失败事件握手，同时最多 2 个；全进程最多 32 个。超出预算时在 101 握手前返回 **403 insufficient_quota**，附 Retry-After 和 X-Should-Retry:false，不占用 WS 槽位。不承诺所有客户端都遵从响应头。
- 已接受的欠费 WS 不协商压缩；读取首帧头后先发送失败通知，再限量丢弃尚在上传的数据，兼容先发完后读取的客户端。最多 8 MiB、连接总期限 2 秒（含关闭握手），不保存历史。这个上限只用于已拒绝的欠费连接，不是正常请求的大小上限。
- 保护状态只保存用户 ID、计数、时间和在途连接数，最多 8192 个用户。容量满时拒绝额外欠费 WS，HTTP 仍快速拒绝并聚合。
- 不写账号/Key 停用状态，不新增余额负缓存。充值沿用认证缓存失效机制，下次鉴权确认有余额立即通过并清除该用户的欠费预算，不等 5 分钟。该内存预算是单进程防护，多实例需入口统一控制。

## Nginx 必须一并落地

只更新二进制不能修复 Nginx 先缓冲整个请求正文的问题。准备的两个片段：

1. `nginx/gn2api-ingress-http.conf` 放入 `http {}`。
2. `nginx/gn2api-ingress-server.conf` 放入 API 虚拟主机的 `server {}`，保留原证书、proxy_pass、WS 头和响应流式配置。

片段默认 Key 30 次/秒、突发 120；来源 IP 100 次/秒、突发 400；Key 连接 2048、IP 连接 4096。线上只读核对发现正常用户配置并发最高 1000，所以没有使用 40/200 之类会收紧现有额度的连接限制，并为闲置 WS 留出余量。抽查 20 分钟的完成日志最高为全站 9 条/秒，但完成日志不等于到达峰值，也不证明最大在途连接数。部署前仍应结合实际到达峰值、共享出口和代理拓扑核对。它们限制请求到达和连接数，不是每秒输出 token。多层 location 若已有 limit_req/limit_conn 或 proxy_request_buffering，会覆盖继承，必须检查最终 nginx -T。

`proxy_request_buffering off` 配合 HTTP/1.1，使应用能够在大正文上传完前拒绝。上传空闲超时 15 秒、请求头 10 秒，不设置回答 WriteTimeout，不改变 SSE/WS 正常生成时间或模型上下文。

入口防刷拒绝为本地 **403 ingress_rate_limited**，与余额不足的 `insufficient_quota`、上游真实 429 区分。`proxy_intercept_errors off` 保证应用原响应不被覆盖。限流错误日志级别设为 info，error_log 应为 warn 或更高，防止拒绝日志再次造成磁盘压力。

CDN/负载均衡环境必须先配置可信代理 CIDR 和真实 IP；不能直接信任任意 X-Forwarded-For，否则能绕过 IP 限制。也不能将代理公共出口当成所有客户的 IP 后盲目套低并发限制。此片段不替代上游网络层的大流量防护。

## 验证和发布顺序

```sh
cd backend
go test -tags=unit ./internal/service ./internal/server/middleware ./internal/handler ./internal/handler/admin -run 'BalanceDenial|CodexBalance|APIKeyAuthResponsesBalance|IngressReject' -count=1
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -tags=unit -o /tmp/middleware.test ./internal/server/middleware
cd ..
python3 deploy/test_balance_denial_ingress.py --binary /tmp/middleware.test --codex /path/to/codex-0.166.0 --output /tmp/balance-acceptance.json
```

隔离验收使用本地 Nginx、假 Key 和不连接上游的 Go fixture。检查未上传 16 MB 声明正文即可返回、入口突发保护、官方客户端 HTTP/WS 两轮（含约 1.5 MB 输入）、继续运行 140 秒，以及充值后放行。不能把它当成已复现远端用户的外层自动任务。

本地验收完成后先发布 GitHub。获得部署授权后备份二进制和 Nginx 配置，先 `nginx -t`，再按用户确认的部署窗口更新并验收真实流量；回滚时两部分一起回滚。不得在发布前偷偷改线上。
