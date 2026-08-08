# KubeJob 中文说明

[中文使用指南](./docs/v3/getting-started.zh-CN.md) · [V3 中文架构](./docs/v3/architecture.zh-CN.md) · [English](./README.md)

KubeJob 是一个面向 .NET 的强类型、可嵌入、分布式后台任务运行时。

V3 的核心原则是 **Single Authority（单一执行权威）**：每个逻辑 Job Queue 只能选择一种 Runtime。

| Runtime | 执行权威 | 强状态/取消 | 正常执行是否依赖数据库 |
| --- | --- | --- | --- |
| `PostgresManaged` | PostgreSQL | 支持 | 是 |
| `BrokerNative` | Message Transport | 当前不支持 | 否 |

## PostgresManaged

PostgreSQL 负责：

- JobRun / JobAttempt
- Claim
- Lease / Renew
- Worker Session / Epoch / Fencing
- Retry Budget
- 持久化取消和任务状态
- `KeyOrdered` / `StrictFifo`

Worker 周期性 Pull PostgreSQL。可配置 RabbitMQ/进程内 WorkAvailable 通知减少发现延迟，但通知只是 Wake Hint，PostgreSQL 才是执行权威。

默认没有配置通知器时，V3 不需要为了一个 Noop Wake 为每个任务额外写一条 `Kj2_Outbox`。

## BrokerNative

BrokerNative 正常热路径：

```text
IJobClient
  -> Transport Publisher
  -> RabbitMQ
  -> Consumer
  -> WorkerExecutionEngine
  -> Handler
  -> ACK / Retry / DLQ
```

正常执行不会创建 PostgreSQL Run/Attempt，也不会调用 Claim、Admission、Lease 或同步数据库 Completion。

当前已经实现 RabbitMQ Adapter；Kafka、SQS、Redis Streams、Pulsar 等仍属于扩展目标，不代表已经实现。

BrokerNative 是 **at-least-once**。网络异常、Worker 崩溃、Retry Handoff 都可能产生重复投递，因此业务副作用必须幂等。

`IdempotencyKey` 当前只是 BrokerNative Message 元数据，KubeJob **没有**基于它自动做 BrokerNative 去重。

`JobHandle.JobId`：

- PostgresManaged：RunId
- BrokerNative：Transport MessageId

当前 `IJobClient.GetStatusAsync` / `CancelAsync` 的强语义只属于 PostgresManaged；V3 尚未实现 BrokerNative History Projection 和 Queued Cancel。

## Job 与 Event

Job Queue 是竞争消费：

```text
Queue
  +-- Worker A
  +-- Worker B
  +-- Worker C
```

Event 是 Topic + Subscription：

```text
order.events / order.created
       |
       +-- business -> Queue -> Worker replicas
       +-- audit    -> Queue -> Worker replicas
       +-- cleanup  -> Queue -> Worker replicas
```

每个 Subscription 收到独立事件副本。Retry/DLQ 只作用于当前 Subscription，不会把 Topic 重新广播给已经成功的订阅者。

## Batch

`EnqueueBatchAsync` 在两种 Runtime 下语义不同：

- PostgresManaged：一个受限数据库事务。
- BrokerNative：不是原子批次。RabbitMQ 可以批量 Publish 后只等待一次 Publisher Confirm，以降低每消息 RTT，但失败时 Broker 可能已经接收部分或全部消息。

因此 BrokerNative Batch 重试仍然必须按照 at-least-once 处理。

## 当前实现边界

已经实现：

- PostgresManaged Run / Attempt / Claim / Lease / Fencing / Status / Cancel / Ordering
- RabbitMQ BrokerNative Job Runtime
- RabbitMQ Event Topic / Subscription Runtime
- RabbitMQ Publisher Confirm / Retry Handoff / DLQ
- BrokerNative Producer Batch Publish 优化
- Scheduler 根据 Runtime 路由到 Managed 或 BrokerNative

尚未实现：

- Kafka / SQS / Redis / Pulsar Adapter
- BrokerNative 强状态 History Projection
- BrokerNative Queued Cancel
- BrokerNative 内置 Idempotency/Dedup Store
- 通用业务数据库 Transactional Outbox 包

继续阅读：

- [V3 中文使用指南](./docs/v3/getting-started.zh-CN.md)
- [V3 中文架构说明](./docs/v3/architecture.zh-CN.md)
- [V3 Architecture](./docs/v3/architecture.md)
- ADR 015：Single Authority Runtime Model

`docs/v2` 继续保留作为历史设计资料，但不再代表当前 Runtime 合同。
