# Responses 余额耗尽与监控分类修复

本次修复不改变余额门槛、在途预留或结算逻辑。余额耗尽时，Responses HTTP 与 WS 握手返回 HTTP 429，`error.type` 和 `error.code` 均为 `insufficient_quota`。已连接 WS 保留同样的终止错误和 1008 关闭；排队心跳已提交 HTTP 200 时发送 `response.failed`，避免客户端只看到断流。普通限流仍保持独立错误类别及原有退避语义。

Responses 的 OpenAI 与兼容网关两个 handler 均使用同一错误构造器。非 Responses 端点保留原协议错误格式。首字规则、前端和账号调度策略不在本次修改范围内。

Ops 将明确余额/额度错误记作 `billing_error`，本地拒绝归用户侧业务限制；有真实上游错误上下文时仍归上游。监控的 Go 与 SQL 分类都补认 `Insufficient account balance`、`INSUFFICIENT_BALANCE`、`insufficient_quota` 和 `API_KEY_QUOTA_EXHAUSTED`。

这是 v1 既有 `quota_or_balance` 类别的漏识别修正，未改变类别定义或判定优先级，因此保留 taxonomy version 1。历史重算必须单独执行，不能靠提升版本号使旧数据退出查询。

## 验证

- 后端 middleware、handler 全包 unit 测试，以及 service/repository 的监控分类测试。
- 本机官方 `codex-cli 0.166.0-alpha.6` 隔离 CODEX_HOME、loopback、虚拟 Key 验证：HTTP 1 次请求后终止；WS 2 次握手（启动探测和本轮请求），无 HTTP 回退；已连接 WS 的额度拒绝也无 HTTP 回退。
- 测试不会调用模型、读取用户 Codex 配置或消耗真实账号额度。任意外层脚本仍可能忽略错误自行继续请求，服务端的错误码无法强制终止外层循环。

原生验证命令（需设置 `GN2API_TEST_CODEX_EXE` 为本机官方可执行文件）：

```text
go test -tags unit ./internal/server/middleware ./internal/handler -run 'Test(APIKeyBalanceNativeCodex|OpenAIWSBillingErrorNativeCodex)' -count=1 -v
```

## 历史分类修复

`repair_balance_error_categories.py` **只生成 SQL，不连接数据库**；默认生成只读预览。时间必须包含时区，每次最多 7 天；结束时间应至少落后当前 10 分钟，避开活跃请求。先用新代码生成 SQL 并预览，核对后才使用 `--apply` 生成修改事务，交由 psql 执行。

```text
python deploy/repair_balance_error_categories.py --start 2026-11-05T00:00:00Z --end 2026-11-05T14:40:00Z
```

工具使用网关原始 SQL 的请求去重和分类规则，逐个核对完整统计桶的旧分类数量。只有旧分类全部数量与留存原始日志吻合时才重写分类：

- `repair`：证据完整，可修正。
- `already_correct`：已正确，无需再次修改。
- `skip_source_mismatch`：原始来源、去重或统计数量不一致，保留现状。
- 没有留存来源、超出范围或只有部分时间覆盖的桶不修改。

修改事务锁定两张错误分类聚合表，限制锁等待和语句时间；失败整体回滚。只修正错误分类数量，不修改原始日志、总请求数、成功数、用量、用户余额、延迟统计或聚合游标。修正后可命中既有的“余额/额度”健康评分排除规则；**全部请求成功率仍按真实成功数/请求数计算**。

可用独立安装的 `@electric-sql/pglite` 验证真实 PostgreSQL 执行。设置 `GN2API_PGLITE_PATH` 和 `GN2API_PYTHON`，执行：

```text
node deploy/repair_balance_error_categories_test.cjs
```

回归覆盖重复请求去重、五种时间粒度、真正权限/限流错误保留、总数守恒、来源缺失保护、只读预览和重复修复幂等性。该依赖只用于隔离测试，不加入项目构建依赖。
