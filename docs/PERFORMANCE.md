# 低内存与高性能设计

## 已在基础代码中采用

- 反射扫描和 handler delegate 构建只在启动发生。
- Worker 是固定槽位，不为每轮 poll 启动无界 Task。
- 本地 Channel、拥有字典、续租 List 均受 MaxConcurrentJobs 限制。
- claim 只取空闲槽位数，Server 再按 DB 已拥有数量二次限流；claim 事务直接创建 Running attempt，省掉每 Job 一次 start HTTP 往返。
- PostgreSQL claim 使用部分索引和 `SKIP LOCKED`；租约、心跳、取消和调度时间以数据库时钟为准。
- 空闲状态是 LISTEN/NOTIFY 长轮询，20 秒超时兜底，不进行 100ms DB 自旋。
- JSON 协议使用 System.Text.Json 源生成上下文。
- `LoggerMessage` 源生成避免热日志模板重复解析和参数装箱。
- Payload 字符串转 UTF-8 使用 ArrayPool；归还时默认不清零以降低 CPU（敏感 Payload 可配置清零）。
- Job Payload 按 Batch 单份存储。

## 不应为了“零分配”做的事

- 不要池化含用户数据且生命周期复杂的任意对象。
- 不要缓存无限 JobRun/Spec/Label 数据。
- 不要把 PostgreSQL 队列复制成每个 Server 的全量内存队列。
- 不要用 fire-and-forget 隐藏 Task 和异常。
- 不要用自定义二进制协议替代 HTTP，除非基准证明 HTTP/2 是瓶颈。

## 必测指标

空闲：

- 每 Worker 每分钟请求数
- Server/Worker Gen0 与 allocated bytes/sec
- PostgreSQL QPS、连接数、CPU

负载：

- claim p50/p95/p99
- 从 AvailableAt 到 Running 的延迟
- 每秒 claim/complete 吞吐
- 单 Server 与多 Server 扩展效率
- 10/100/1000 Worker 下的通知风暴
- 1 KiB、64 KiB、1 MiB Payload 的分配和延迟

可靠性：

- 旧 attempt 完成被拒绝的比例应为 100%
- 无任务丢失；允许按语义出现重试重复
- Worker RSS 在固定并发下不随运行总数增长
