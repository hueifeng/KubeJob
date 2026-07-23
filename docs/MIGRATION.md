# 迁移与发布

1. 备份 PostgreSQL；在 staging 对生产规模副本演练 SQL 和锁时间。
2. 先发布兼容旧模式的代码，`RuntimeMode=LegacyDispatcher`。
3. 应用 `002_distributed_runtime_v2.sql`。
4. 发布支持 V2 的 Worker，但先不接生产队列。
5. 启动两个 V2 Server 和 canary Worker，验证 claim/renew/complete。
6. 停止旧 Dispatcher、旧 Cron、旧 NodeHealth 的 run reset 后再切主队列。
7. 保留回滚窗口；V2 表和列先不要删除。

不可同时运行：

- 旧 Dispatcher 与 V2 pull claim
- 旧 `ResetOfflineNodeRunsAsync` 与 lease reaper
- 旧 Worker 无 token 状态上报与 V2 fenced complete

迁移 SQL 中时间列转换假设旧值为 UTC。若历史数据不是 UTC，必须修改 `AT TIME ZONE`。
