# KubeJob

[中文](./README_zh.md) · [V3 Getting Started](./docs/v3/getting-started.md) · [V3 Architecture](./docs/v3/architecture.md) · [中文使用指南](./docs/v3/getting-started.zh-CN.md) · [中文架构](./docs/v3/architecture.zh-CN.md)

KubeJob is a typed, embeddable, distributed background-job runtime for .NET.

V3 is built around **Single Authority** semantics: each logical Job Queue is either `PostgresManaged` or `BrokerNative`. KubeJob provides at-least-once execution and does not claim exactly-once external side effects.

## Runtime modes

| Runtime | Execution authority | Durable Run/Attempt | Strong status/cancel | Normal execution DB dependency |
| --- | --- | --- | --- | --- |
| `PostgresManaged` | PostgreSQL | Yes | Yes | Yes |
| `BrokerNative` | Message transport | No | No | No |

`PostgresManaged` owns Claim, Attempt, Lease, Fencing, durable retry/cancellation and database ordering. An optional work-available notifier may reduce claim latency, but PostgreSQL polling remains the correctness path.

`BrokerNative` publishes a self-contained message directly to the configured transport. The worker executes it through the shared `WorkerExecutionEngine` and the transport owns ACK/redelivery/retry/DLQ. The normal BrokerNative path does not create or claim a PostgreSQL Run.

RabbitMQ is the first implemented BrokerNative adapter. Kafka, SQS, Redis Streams and other transports are extension targets, not currently implemented features.

## Run locally

Start the development PostgreSQL/RabbitMQ stack:

```bash
bash scripts/dev-stack.sh up
```

Run the unified PostgresManaged sample:

```bash
bash scripts/run-unified-sample.sh
```

On Windows use the equivalent `scripts/*.ps1` commands. The sample Dashboard is available at `http://localhost:5041/admin/jobs` and RabbitMQ management at `http://localhost:15672`. Included credentials are development-only.

## Define a typed Job

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

## PostgresManaged

A unified host can keep the control plane and Managed worker in one process without localhost HTTP:

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

Submit and observe a durable Run:

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

## RabbitMQ BrokerNative

Route a Queue to RabbitMQ in producer/control-plane composition:

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

Register the BrokerNative worker data plane:

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

Business code still uses `IJobClient.EnqueueAsync`. For BrokerNative, `JobHandle.JobId` is the transport MessageId rather than a durable Run id.

BrokerNative is at-least-once. `IdempotencyKey` is transported as metadata, but V3 does **not** currently provide a BrokerNative deduplication store. Make external side effects idempotent.

`IJobClient.GetStatusAsync` and `CancelAsync` currently provide the strong PostgresManaged contract only; V3 does not yet contain a BrokerNative history projection or queued-cancel protocol.

## Batch submission

`EnqueueBatchAsync` deliberately has runtime-specific guarantees:

- PostgresManaged: one bounded database transaction.
- BrokerNative: non-atomic broker publication. A capable transport can publish the application batch behind one durability confirmation for throughput, but an error can occur after some or all messages were accepted.

A BrokerNative batch retry must therefore be treated as at-least-once.

## Event Pub/Sub

Jobs and Events have different semantics. A Job Queue is competing-consumer work; an Event Topic fans out to independent Subscriptions.

```text
order.events / order.created
       |
       +-- business -> queue -> worker replicas
       +-- audit    -> queue -> worker replicas
       +-- cleanup  -> queue -> worker replicas
```

Each Subscription owns independent retry/DLQ state. A failing subscriber never republishes the Topic and therefore does not replay already-successful sibling subscriptions.

```csharp
var orderCreated = EventKey<OrderCreated>.Create("order.events", "order.created");

services.AddKubeJobEventHandler<OrderCreated, AuditOrderCreated>(
    orderCreated,
    subscription: "audit");

await eventBus.PublishAsync(orderCreated, new OrderCreated(orderId));
```

## Dashboard

`AddKubeJobDashboard()` provides operational views for Managed Runs/Attempts, workers, queues and schedules. It is read-only by default and hides payloads by default. Bind it to an application authorization policy before production use.

BrokerNative transport topology is deliberately not modeled as PostgreSQL Run state.

## Current capability boundary

Implemented:

- PostgresManaged Run / Attempt / Claim / Lease / Fencing
- durable Managed status, cancellation, retry and ordering
- RabbitMQ BrokerNative Job runtime
- RabbitMQ Event Topic / Subscription runtime
- publisher confirms, retry handoff and DLQ
- BrokerNative producer batch-publish optimization
- schedules routed to either runtime authority

Not implemented:

- Kafka/SQS/Redis/Pulsar transport adapters
- BrokerNative strong history/status projection
- BrokerNative queued cancellation
- built-in BrokerNative idempotency/deduplication store
- a general application-database transactional Outbox package

See [V3 Getting Started](./docs/v3/getting-started.md), [V3 Architecture](./docs/v3/architecture.md), and ADR 015. `docs/v2` is retained as historical design material and is not the current runtime contract.

## PostgreSQL schema compatibility

The current schema still contains compatibility fields such as `DeliveryProfile` and `TransportId`. New PostgresManaged writes normalize these to Pull/null; active runtime selection is controlled by `QueueRuntimeMode`. These columns can be removed later through an explicit schema migration.

## License

MIT
