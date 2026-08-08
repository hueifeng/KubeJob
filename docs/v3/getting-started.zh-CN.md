# KubeJob V3 使用指南

KubeJob V3 有两种明确的 Job Runtime。Runtime 由部署侧按逻辑 Queue 配置，业务代码仍然只提交 Job，不需要在每次调用里选择 RabbitMQ 或 PostgreSQL。

## 1. 定义强类型 Job

```csharp
public sealed record SendEmail(string To, string Subject, string Body);

public sealed class SendEmailJob : IKubeJob<SendEmail>
{
    public ValueTask ExecuteAsync(
        SendEmail payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        // 业务逻辑
        return ValueTask.CompletedTask;
    }
}

var sendEmail = new JobKey<SendEmail>("mail.send");
services.AddKubeJobHandler<SendEmailJob, SendEmail>(sendEmail);
```

## 2. PostgresManaged

需要强状态、Lease/Fencing、持久化取消、数据库顺序控制时使用 PostgresManaged。

统一部署模式可以把 Control Plane 和 Managed Worker 注册在同一个进程：

```csharp
services.AddKubeJob(
    server => server.UsePostgreSql(connectionString),
    worker =>
    {
        worker.WorkerId = "mail-worker";
        worker.Queues = new List<string> { "mail" };
        worker.MaxConcurrentJobs = 32;
    });
```

业务提交：

```csharp
var handle = await jobs.EnqueueAsync(
    sendEmail,
    new SendEmail("user@example.com", "Welcome", "Hello"),
    new JobEnqueueOptions
    {
        Queue = "mail",
        IdempotencyKey = "welcome:user-42",
        MaxAttempts = 5,
        Timeout = TimeSpan.FromMinutes(2)
    });

var status = await jobs.GetStatusAsync(handle.JobId);
await jobs.CancelAsync(handle.JobId, "no longer needed");
```

PostgreSQL Pull 是正确性路径。RabbitMQ 或进程内 WorkAvailable 通知只能降低发现延迟，不拥有执行权。

## 3. RabbitMQ BrokerNative

高吞吐、允许 at-least-once、希望 Broker 直接拥有投递/重试时使用 BrokerNative。

Producer / Control Plane：

```csharp
services.AddKubeJobServer();
services.ConfigureKubeJobQueueRuntimes(options =>
{
    options.Queues["mail-fast"] = new QueueRuntimeRoute
    {
        Mode = QueueRuntimeMode.BrokerNative,
        TransportId = RabbitMqBrokerNativePublisher.Id
    };
});
services.AddRabbitMqKubeJobBrokerNativeTransport(options =>
{
    options.ConnectionString = rabbitMqConnectionString;
});
```

BrokerNative Worker：

```csharp
services.AddKubeJobBrokerNativeWorker(options =>
{
    options.WorkerId = "mail-fast-worker";
    options.Queues = new List<string> { "mail-fast" };
    options.MaxConcurrentJobs = 128;
});
services.AddKubeJobHandler<SendEmailJob, SendEmail>(sendEmail);
services.AddRabbitMqKubeJobBrokerNativeConsumer(options =>
{
    options.ConnectionString = rabbitMqConnectionString;
});
```

业务仍然使用同一个 `IJobClient`：

```csharp
var handle = await jobs.EnqueueAsync(
    sendEmail,
    new SendEmail("user@example.com", "Welcome", "Hello"),
    new JobEnqueueOptions
    {
        Queue = "mail-fast",
        MaxAttempts = 5,
        Timeout = TimeSpan.FromMinutes(2)
    });
```

BrokerNative 中 `handle.JobId` 是 Transport MessageId，不会自动创建 PostgreSQL Run。当前 V3 也没有 BrokerNative History Projection 和强 Queued Cancel。

BrokerNative 是 at-least-once。`IdempotencyKey` 可以作为 Message 元数据传递，但 KubeJob 当前**不会**基于它自动做 BrokerNative 去重；外部副作用需要业务自己保证幂等。

## 4. Batch

```csharp
await jobs.EnqueueBatchAsync(
    sendEmail,
    items.Select(item =>
        (item, (JobEnqueueOptions?)new JobEnqueueOptions { Queue = "mail-fast" }))
        .ToArray());
```

PostgresManaged 使用一个受限数据库事务。

BrokerNative Batch 不是原子事务。RabbitMQ Adapter 可以先批量 Publish，再只做一次 Publisher Confirm 以减少每消息 RTT；如果 Publish 返回异常，Broker 可能已经接收部分甚至全部消息，因此重试仍然要按 at-least-once 处理。

## 5. Event

Event 是 Pub/Sub，不是 Job Queue 的竞争消费。

```csharp
var orderCreated = EventKey<OrderCreated>.Create("order.events", "order.created");

services.AddKubeJobEventHandler<OrderCreated, AuditOrderCreated>(
    orderCreated,
    subscription: "audit");
```

Event Worker：

```csharp
services.AddKubeJobBrokerNativeWorker(options =>
{
    options.WorkerId = "order-audit-worker";
    options.MaxConcurrentJobs = 64;
});
services.AddRabbitMqKubeJobEventConsumer(options =>
{
    options.ConnectionString = rabbitMqConnectionString;
});
```

发布：

```csharp
await eventBus.PublishAsync(orderCreated, new OrderCreated(orderId));
```

每个 Subscription 都拥有独立 Queue 和事件副本。Retry/DLQ 只作用于当前 Subscription，某个订阅失败不会让已经成功的其他订阅重复消费。

## 6. 当前能力边界

已经实现：

- PostgresManaged Run / Attempt / Lease / Fencing / Status / Cancel / Ordering
- RabbitMQ BrokerNative Job Runtime
- RabbitMQ Event Topic / Subscription Runtime
- RabbitMQ Retry / Publisher Confirm / DLQ
- BrokerNative Producer Batch Publish 优化
- Scheduler 按 Runtime 路由到 Managed 或 BrokerNative

尚未实现：

- Kafka / SQS / Redis / Pulsar Adapter
- BrokerNative 强状态 History Projection
- BrokerNative Queued Cancel
- BrokerNative 内置 Idempotency/Dedup Store
- 通用业务数据库 Transactional Outbox 包

继续阅读：[V3 架构说明](./architecture.zh-CN.md) 与 ADR 015。
