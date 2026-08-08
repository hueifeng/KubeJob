# KubeJob 中文文档

KubeJob 是一个强类型、可嵌入、支持分布式部署的 .NET 后台任务运行时。

当前运行时采用 **V3 Single Authority（单一执行权）** 模型：每个逻辑 Queue 只能选择一种执行权，不再让 RabbitMQ 交付后再由 PostgreSQL 二次批准执行。

## 两种 Job Runtime

### PostgresManaged

PostgreSQL 是任务执行权和状态事实源，负责：

- `JobRun` / `JobAttempt`
- Claim / Lease / Lease Renew
- Worker Session / Epoch / Fencing
- Durable Cancel
- 强一致任务状态
- `KeyOrdered` / `StrictFifo`
- Managed Retry / Continuation / Compensation

Worker 只在有空闲并发槽位时从 PostgreSQL Claim 任务。

为了降低提交热路径写放大，**立即可执行的新 Run 不再每条写一条 `Kj2_Outbox` WorkAvailable 记录**。控制面先提交 Run，再通过进程内的 `ManagedWorkAvailableDispatcher` 按逻辑 Queue 合并并异步发送 best-effort wake。即使 wake 丢失，Worker 仍会通过 PostgreSQL polling 发现任务，因此不会丢 Job。

未来 `NotBefore` 任务，以及显式 Retry/Requeue 等恢复场景，目前仍保留 durable WorkAvailable outbox；这是兼容/延迟唤醒路径，不是执行权。

### BrokerNative

消息中间件是任务交付和重试事实源：

```text
IJobClient
   ↓
Transport Publisher
   ↓
RabbitMQ
   ↓
Worker
   ↓
WorkerExecutionEngine
   ↓
Handler
   ↓
ACK / Retry / DLQ
```

BrokerNative 正常执行链路不会创建或查询 PostgreSQL `JobRun`，也不会执行数据库 Admission、Lease 或同步 Completion 写入。目前已经实现的 BrokerNative Transport 是 **RabbitMQ**；Kafka、SQS、Redis Streams、Pulsar 等是扩展目标，不代表已经实现。

BrokerNative 使用 **at-least-once** 交付语义，业务副作用应当具备幂等性。当前尚未提供 BrokerNative Inbox/Deduplication，因此 `JobEnqueueOptions.IdempotencyKey` 只属于 PostgresManaged 语义，BrokerNative 会明确拒绝该选项。

## Job 与 Event

KubeJob 将两类消息语义分开：

- **Job Queue**：同一个 Queue 下的多个 Worker 副本竞争消费，一条 Job 只由其中一个 Worker 处理。
- **Event Topic**：一个 Event 可以有多个独立 Subscription；每个 Subscription 有自己的消费队列，Subscription 内部的 Worker 副本竞争消费。

Event 重试是 Subscription 级别的，不会把失败事件重新发布回 Topic，因此已经成功的其他 Subscription 不会重复消费。

## JobHandle 与状态

`IJobClient.EnqueueAsync` 返回 `JobHandle`：

- PostgresManaged：`JobId` 是持久化 `JobRun` Id，支持强状态查询和 Durable Cancel。
- BrokerNative：`JobId` 是消息 Id，`RuntimeMode` 为 `BrokerNative`，不会自动拥有 PostgreSQL Run 状态。

调用方可通过：

```csharp
handle.RuntimeMode
handle.TransportId
handle.SupportsStrongStatus
handle.SupportsStrongCancellation
```

判断当前任务具备的能力。

## 文档入口

> 仓库中的 `docs/v2` 路径目前为兼容已有链接而保留；其中核心文档正在按 V3 语义更新，目录名不再代表运行时仍是 V2。

- [中文使用指南](./docs/v2/getting-started.zh-CN.md)
- [架构说明](./docs/v2/architecture.md)
- [Dashboard 与安全边界](./docs/v2/security.md)
- [ADR 015：V3 Single Authority Runtime Model](./docs/adr/015-v3-single-authority-runtime-model.md)
- [英文首页](./README.md)

KubeJob 提供 **at-least-once execution**，不宣称外部业务副作用 exactly-once。
