# KubeJob V3 架构说明

KubeJob V3 的核心原则只有一条：**一个逻辑 Queue 只能有一个执行权威（Authority）**。

## 两种 Runtime

| Runtime | 执行权威 | Run/Attempt | 强状态/取消 | 正常执行是否依赖数据库 |
| --- | --- | --- | --- | --- |
| `PostgresManaged` | PostgreSQL | 有 | 有 | 是 |
| `BrokerNative` | 消息 Transport | 无 | 无 | 否 |

Runtime Mode 和 Transport 是两个独立概念。当前 BrokerNative 已实现 RabbitMQ Adapter；Kafka、SQS、Redis Streams 等只是未来扩展目标，不代表已经实现。

## PostgresManaged

PostgreSQL 同时负责排队状态和执行所有权：

```text
IJobClient
  -> JobRun
  -> Worker Claim
  -> Attempt + Lease + Fence
  -> WorkerExecutionEngine
  -> Handler
  -> 数据库完成状态
```

适合需要以下能力的任务：

- 强一致任务状态
- 持久化取消
- Claim / Lease / Fencing
- Retry Budget
- `KeyOrdered` / `StrictFifo`
- Continuation / Compensation

Worker 周期性 Pull PostgreSQL。可以额外配置 WorkAvailable 通知缩短发现延迟，但通知只是 Wake Hint，不拥有任务执行权。通知丢失后 Worker 仍能靠 Poll 恢复。

默认未配置通知器时，不需要为每个 Managed Job 再写一条无实际作用的 `WorkAvailable` Outbox。

## BrokerNative

BrokerNative 由 Transport 负责投递和重试：

```text
IJobClient
  -> IMessageTransportPublisher
  -> Broker
  -> Transport Consumer
  -> WorkerExecutionEngine
  -> Handler
  -> ACK / Retry / DLQ
```

正常热路径不会：

- 创建 PostgreSQL `JobRun`
- 创建 Attempt
- 调用数据库 Claim / Admission
- 创建 Lease
- 在 ACK 前同步写数据库完成状态

BrokerNative 是 **at-least-once**。发布异常、网络断开、Worker 崩溃和 Retry Handoff 都可能产生重复投递，因此业务副作用必须具备幂等性。

`IdempotencyKey` 当前会被带入 BrokerNative Message，但 **KubeJob V3 目前没有 BrokerNative 去重存储**，不要把它理解成框架已经自动去重。

`JobHandle.JobId` 在 PostgresManaged 中是 RunId；在 BrokerNative 中是 Transport MessageId。当前 `IJobClient.GetStatusAsync` / `CancelAsync` 的强语义只属于 PostgresManaged；V3 还没有 BrokerNative History Projection 或 Queued Cancel 协议。

## 共享执行引擎

两种 Runtime 最终都进入 `WorkerExecutionEngine`：

- 创建 DI Scope
- Payload 反序列化
- Middleware
- Handler 调用
- Timeout / Cancellation
- Telemetry
- 异常分类

数据库 Lease、Broker ACK 等协调逻辑都不进入执行引擎。

Worker 停止/Drain 不应该被持久化成一次假的 Job Cancel，而应该继续向上抛给 Runtime Coordinator，让 PostgresManaged Lease Recovery 或 Broker Redelivery 恢复执行所有权。只有 KubeJob 自己的 Timeout Token 真正触发时，`OperationCanceledException` 才会被归类为 `TimedOut`；业务/下游自己抛出的 OCE 不再误判成超时。

## Job Queue

Job 是竞争消费：

```text
logical queue
    |
    +-- Worker A
    +-- Worker B
    +-- Worker C
```

同一条 Job 消息由某一个 Worker 完成。KubeJob 不为每个 Worker 创建私有 Queue。

RabbitMQ BrokerNative 默认允许多个 Consumer 并发执行，因此虽然 RabbitMQ Queue 本身有投递顺序，也**不能宣称业务执行结果有序**。需要顺序时，应由明确的 Transport-native Partition/Ordering 策略提供，而不是重新引入 PostgreSQL Admission。

## Event Pub/Sub

Event 和 Job 是不同语义：

```text
Topic: order.events
RoutingKey: order.created
       |
       +-- business -> Queue -> Worker replicas
       +-- audit    -> Queue -> Worker replicas
       +-- cleanup  -> Queue -> Worker replicas
```

每个 Subscription 拥有独立 Queue，因此每个 Subscription 都收到一份事件；同一个 Subscription 内的多个 Worker Replica 竞争消费。

Retry / DLQ 必须以 Subscription 为边界。`business` 失败时只能重试 `business`，不能重新 Publish Topic，否则已成功的 `audit/cleanup` 会重复执行。

### Durable Subscription Provisioning

RabbitMQ Exchange 自己不会替“未来尚不存在的 Subscription”保存事件。**如果希望 Worker 离线期间事件继续在 Subscription Queue 中积压，那么该 durable Queue 必须在事件发布之前已经创建。**

普通 Event Worker 使用：

```csharp
services.AddKubeJobEventHandler<OrderCreated, AuditOrderCreated>(
    EventKey<OrderCreated>.Create("order.events", "order.created"),
    "audit");

services.AddRabbitMqKubeJobEventConsumer(options =>
{
    options.ConnectionString = rabbitMqConnectionString;
});
```

Consumer 会在 `BasicConsume` 前声明自己的 Topic / Subscription / Retry / DLQ，并保持后台重连模型；RabbitMQ 临时不可用不会把正常 Worker 启动改成一个新的 fail-fast 依赖。

如果要求在 Worker 启动之前就完成 durable Subscription 创建，可以使用 topology-only 注册和独立 Provisioner：

```csharp
services.AddKubeJobEventSubscription(
    EventKey<OrderCreated>.Create("order.events", "order.created"),
    "audit");

services.AddRabbitMqKubeJobEventTopologyProvisioner(options =>
{
    options.ConnectionString = rabbitMqConnectionString;
});
```

Provisioner 适合部署/迁移步骤，它会 fail-fast：如果要求的 RabbitMQ 拓扑没有真正创建成功，部署步骤不应该伪装成成功。

如果在某个 Subscription Queue 从未被 provision 的情况下向 Topic 发布事件，那么遵循正常 Pub/Sub 语义：不存在 Queue，也就不存在该订阅者可持久保存的那份事件。

### RabbitMQ Job/Event 物理命名隔离

Job 与 Event 物理拓扑不能发生别名冲突。为了不破坏已经存在的 BrokerNative Job Queue，Job 物理名继续保持：

```text
kubejob.<logical-job-queue>
```

Event 物理对象则使用逻辑名不能包含的 `~` 作为结构化边界，例如：

```text
kubejob.eventx~order.events
kubejob.eventsub~order.events~audit
kubejob.eventretryq~order.events~audit
kubejob.eventdlq~order.events~audit
```

逻辑 Queue / Topic / Subscription 本身不允许 `~`，RabbitMQ 的 `QueuePrefix` 和 `ExchangeName` 也禁止 `~`。这样可以避免：

- Job Queue `order.audit` 与 Event `(Topic=order, Subscription=audit)` 落到同一个 Queue；
- Event Topic `jobs` 与默认 Job Exchange `kubejob.jobs` 同名但 Exchange Type 不同；
- Subscription 主 Queue、Retry Queue、DLQ 因后缀组合产生歧义碰撞。

### Event 拓扑升级说明

本次 post-merge hardening **只修改 Event 物理拓扑名称，不修改现有 BrokerNative Job Queue 名称**。

如果环境已经使用合并到 main 的早期 V3 Event Queue，升级时应明确处理旧 Queue 中的积压消息：

1. 暂停/静默受影响 Topic 的 Event Publisher；
2. 使用目标 Subscription 定义 provision 新 Event 拓扑；
3. 检查旧 Subscription / Retry / DLQ 是否还有 Pending Message；
4. 根据业务语义选择 Drain、Replay 或明确丢弃；
5. 启动使用新拓扑的 Consumer；
6. 恢复 Publisher；
7. 确认旧拓扑不再需要后再删除。

KubeJob 不自动把旧 RabbitMQ Queue 中的消息搬到新 Queue，因为自动搬迁等于替业务擅自决定 Replay / Duplicate 语义。

## RabbitMQ Retry Handoff

BrokerNative Job 执行失败需要重试时：

1. 先发布 Retry Copy；
2. 等 Publisher Confirm；
3. Broker 确认 Retry Copy 已接收后，才 ACK 原消息。

如果中间发生基础设施错误，原消息保持未 ACK / Requeue，从而保持 at-least-once。

## Batch 提交

`IJobClient.EnqueueBatchAsync` 在两种 Runtime 下保证不同：

- PostgresManaged：一个受限数据库事务。
- BrokerNative：不是原子批次。Transport 可以把多条 Publish 合并为一次 Confirm 以提高吞吐，但异常发生时 Broker 可能已经接收部分甚至全部消息。

同一个 `MaxSubmissionBatchSize` 同时限制两种 Runtime。对 BrokerNative 而言，这不仅是 API 保护，也限制一次性序列化内存、Publisher Lock 持有时间以及一次 Confirm Window 的大小。

RabbitMQ Publisher 会在一个 Channel 生命周期内缓存已经成功声明的 Job Queue / Event Topic，避免每条消息都重复支付同步 QueueDeclare / ExchangeDeclare 的 Broker RPC；Channel 重建或 mandatory Job Publish 不可路由时会清空缓存并重新声明。

因此 BrokerNative Batch 重试仍必须按照 at-least-once 处理。

## Scheduler

Schedule 定义仍由 Control Plane 持久化：

- PostgresManaged：触发时创建 Durable Run。
- BrokerNative：触发时直接 Publish Message；只有 Publish Confirm 成功后才推进 Schedule Cursor。

BrokerNative Schedule Occurrence 的 MessageId 基于 `(ScheduleId, ScheduledFor)` 稳定生成。Publish Confirm 成功后、Cursor Commit 前发生崩溃时，最多会再次发布**同一个 MessageId**，而不是先推进游标导致任务丢失。

依赖强 Run 状态的策略继续属于 PostgresManaged。

## Schema 兼容边界

当前 PostgreSQL Schema 暂时保留 `DeliveryProfile`、`TransportId` 等兼容字段。新的 Managed 写入统一为 Pull/null，这些字段已经不再决定 Broker 执行路径。后续可以通过单独的 Major Schema Migration 删除。

`docs/v2` 保留作为历史设计资料；当前实现以 `docs/v3` 和 ADR 015 为准。
