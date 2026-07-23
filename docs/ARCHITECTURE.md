# 分布式执行架构

## 1. 保证级别

KubeJob V2 保证 at-least-once。Worker 可能已经完成外部副作用，但在提交完成状态前崩溃；系统只能重试。Job 作者必须使用 RunId/业务幂等键、事务 inbox/outbox 或目标系统的去重约束。

## 2. 为什么移除中央 Dispatcher 热路径

旧模型每秒扫描 Pending、读取全部 Node/Spec、解析 JSON、排序并逐条更新；多 Server 还需要领导锁。V2 由 Worker 拉取：

- 每次只请求真实空闲槽位；
- SQL 使用 `FOR UPDATE SKIP LOCKED` 拆分并发消费者；
- DB 锁定 Worker 行并重新计算已拥有任务数，恶意或重复 claim 也不能超容量；
- Server 无状态，可水平扩容。

## 3. 三层 fencing

- `WorkerId`：稳定逻辑节点名。
- `SessionId + SessionEpoch`：精确到进程；同 WorkerId 重启会让旧进程失效。
- `Attempt + LeaseToken`：精确到某次执行；过期 attempt 不能完成新 attempt。

所有写入必须验证当前 session 和 token。租约有效期、心跳和 Cron due 判断使用 PostgreSQL 时钟；应用节点本地时钟只用于等待、日志和 Worker 侧执行超时。

## 4. 状态机

```text
Pending --atomic claim--> Running --fenced complete--> Succeeded
   ^                    |                  |              \-> Failed
   |                    |                  |              \-> Canceled
   +---- retry/backoff -+------ lease expiry ------------+
```

- Pending 可立即取消；Running 通过续租响应协作取消。Assigned 仅为旧版兼容状态，V2 不再产生。
- Running 取消先设置 `CancelRequestedAt`，续租响应通知 Worker；若 Worker 不响应，由租约过期收敛。
- 失败或租约过期在 `Attempt <= MaxRetries` 时重排同一个逻辑 Run。

## 5. Cron、分片和广播

Cron 每次只锁一个 due spec，事务短且不会阻塞其他 Server。

- 首次代码注册只初始化 `NextRunTime`，不会立即意外执行。
- 游标推进与 deterministic Run 创建一起提交。
- missed fires 默认 coalesce；后续可增加 CatchUpPolicy。
- Sharding 创建固定 BatchSize 个独立 Run，不承诺同时启动。
- Broadcast 在 Repeatable Read 快照中固定目标 `(WorkerId, SessionEpoch)`；集群成员变化不改变已有 Batch。

## 6. 版本滚动

Worker 发布 `JobType / HandlerVersion / PayloadSchemaVersion`。Run 在创建时快照：

- JobType
- Timeout/Retry
- NodeSelector
- Queue/Priority
- RequiredHandlerVersion
- PayloadSchemaVersion

JobSpec 后续修改只影响新 Run。claim 必须匹配具体能力，旧 Worker 不会误吃新格式 Payload。

## 7. 过载边界

- 单次 claim 上限 256。
- Worker 拥有集合上限等于 MaxConcurrentJobs。
- 单 Job 分片/广播上限 4096。
- 每个后台查询都有 LIMIT。
- Payload 默认 1 MiB、硬上限 16 MiB；更大数据应放对象存储。
