# KubeJob V3 Getting Started

KubeJob V3 supports two explicit Job runtimes. Choose the runtime per logical Queue in deployment configuration; business callers still enqueue a logical Job and do not select infrastructure per request.

## 1. Define and register a typed Job

```csharp
public sealed record SendEmail(string To, string Subject, string Body);

public sealed class SendEmailJob : IKubeJob<SendEmail>
{
    public ValueTask ExecuteAsync(
        SendEmail payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        // business logic
        return ValueTask.CompletedTask;
    }
}

var sendEmail = new JobKey<SendEmail>("mail.send");
services.AddKubeJobHandler<SendEmailJob, SendEmail>(sendEmail);
```

## 2. PostgresManaged

Use PostgresManaged when you need durable Run/Attempt state, strong status/cancellation, leases/fencing or database-owned ordering.

A unified host can register the server/control plane and managed worker together:

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

Then enqueue normally:

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

PostgreSQL polling is the correctness path. Optional RabbitMQ/in-process work-available notifications can reduce claim latency but never own execution.

## 3. BrokerNative with RabbitMQ

Use BrokerNative for high-throughput background work where the broker should own delivery/retry and the handler can tolerate at-least-once execution.

Producer/control-plane composition:

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

BrokerNative worker:

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

Enqueue through the same client API:

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

For BrokerNative, `handle.JobId` is the transport MessageId. V3 does not currently create a durable KubeJob Run/history projection for it, and strong queued cancellation is not implemented.

BrokerNative is at-least-once. `IdempotencyKey` may be carried as message metadata but KubeJob does not currently deduplicate BrokerNative execution with it; make external side effects idempotent in the business layer.

## 4. Batch submission

```csharp
await jobs.EnqueueBatchAsync(
    sendEmail,
    items.Select(item =>
        (item, (JobEnqueueOptions?)new JobEnqueueOptions { Queue = "mail-fast" }))
        .ToArray());
```

PostgresManaged uses one bounded database transaction. BrokerNative is not atomic; RabbitMQ can publish a batch before one publisher confirmation for throughput, but an error can be returned after a subset or all messages were accepted.

## 5. Events

Events are publish/subscribe, not competing Job Queue semantics.

```csharp
var orderCreated = EventKey<OrderCreated>.Create("order.events", "order.created");

services.AddKubeJobEventHandler<OrderCreated, AuditOrderCreated>(
    orderCreated,
    subscription: "audit");
```

An event worker uses the BrokerNative execution core plus the RabbitMQ event consumer:

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

Publish through `IEventBus`:

```csharp
await eventBus.PublishAsync(orderCreated, new OrderCreated(orderId));
```

Each Subscription gets an independent queue/copy. Retry and DLQ stay inside that Subscription, so one failing subscriber does not replay successful siblings.

## 6. Current capability boundary

Implemented today:

- PostgresManaged Run/Attempt/Lease/Fencing/status/cancellation/ordering
- BrokerNative RabbitMQ Job execution
- RabbitMQ Event Topic/Subscription runtime
- RabbitMQ retry, publisher confirms and DLQ
- BrokerNative producer batch publish optimization
- Schedule routing to Managed or BrokerNative

Not implemented today:

- Kafka/SQS/Redis/Pulsar adapters
- BrokerNative strong status/history projection
- BrokerNative queued cancellation
- built-in BrokerNative idempotency/deduplication store
- general application/business-database transactional Outbox package

See [V3 architecture](./architecture.md) and ADR 015 for the design contract.
