# 关键设计决策

| 决策 | 选择 | 原因 |
|---|---|---|
| 执行保证 | at-least-once | 任意外部副作用无法通用 exactly-once |
| 分配模型 | Worker pull | 去掉中央 Dispatcher 瓶颈与领导切换暂停 |
| 并发 claim | SKIP LOCKED | 多 Server 共享 PostgreSQL 队列 |
| 所有权 | session epoch + lease token | 防旧进程和旧 attempt 写回 |
| 时间权威 | PostgreSQL clock | 控制面多节点时钟偏差不参与租约正确性 |
| 开始语义 | claim 即 Running | 固定槽位下省掉每 Job 一次 start 往返 |
| 重试 | 同一逻辑 Run 新 Attempt | 保留稳定 RunId，审计每次 attempt |
| Broadcast | 触发时 session 快照 | 扩缩容不改变已生成批次 |
| Payload | Batch 单份 | 避免 Sharding/Broadcast 复制 JSON |
| 空闲等待 | LISTEN/NOTIFY + timeout | 低 QPS，通知只作提示不作正确性依据 |
| 配置注册 | CreateIfMissing | Worker 启动不覆盖人工配置 |
| 兼容策略 | Legacy/V2 显式模式 | 可灰度和回滚，禁止两套调度并行 |
