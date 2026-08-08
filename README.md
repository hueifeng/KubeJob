# KubeJob

[中文说明](./README_zh.md) · [中文指南](./docs/v2/getting-started.zh-CN.md) · [Getting Started](./docs/v2/getting-started.md) · [Architecture](./docs/v2/architecture.md) · [ADR 015](./docs/adr/015-v3-single-authority-runtime-model.md)

KubeJob is a typed, embeddable, distributed background-job runtime for .NET.

The current V3 architecture follows a **Single Authority** rule: every logical Job Queue chooses exactly one execution authority.

- **PostgresManaged** — PostgreSQL owns `JobRun`, `JobAttempt`, Claim, Lease, Worker Session fencing, durable cancellation, strong status, managed retry and managed ordering.
- **BrokerNative** — the selected message transport owns delivery, redelivery, retry, acknowledgement and dead-letter handling. Normal BrokerNative execution does not create or claim a PostgreSQL Run.

RabbitMQ is the first implemented BrokerNative transport. Other transports are extension targets, not built-in features today.

KubeJob provides **at-least-once execution**. It does not claim exactly-once external side effects.

## Run locally

The repository includes a PostgreSQL + RabbitMQ development stack:

```bash
bash scripts/dev-stack.sh up
bash scripts/run-unified-sample.sh
```

On Windows use:

```powershell
pwsh scripts/dev-stack.ps1 -Action up
pwsh scripts/run-unified-sample.ps1
```

The sample Dashboard is available at `http://localhost:5041/admin/jobs`; RabbitMQ management is available at `http://localhost:15672`. Included credentials are development-only.

## Define a typed job

```csharp
public sealed record SendEmail(string To, string Subject, string Body);

[KubeJob("mail.send")]
public sealed class SendEmailJob : IKubeJob<SendEmail>
{
    private readonly IEmailSender _sender;

    public SendEmailJob(IEmailSender sender) => _sender = sender;

    public ValueTask ExecuteAsync(
        SendEmail payload,
        JobExecutionContext context,
        CancellationToken cancellationToken) =>
        _sender.SendAsync(payload, cancellationToken);
}
```

The source generator creates a strongly typed key such as `Jobs.SendEmail`. Business dependencies use constructor injection; middleware and handlers share the transport-neutral `WorkerExecutionEngine`.

## PostgresManaged

PostgresManaged is the default Queue runtime.

```csharp
builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();
builder.Services.AddKubeJob(
    configureServer: server => server.UsePostgreSql(connectionString),
    configureWorker: worker =>
    {
        worker.WorkerId = Environment.MachineName;
        worker.MaxConcurrentJobs = 16;
        worker.Queues = new List<string> { "mail" };
        worker.BuildId = "mailer-2026.08";
    });
```

Enqueue a durable managed Run:

```csharp
var handle = await jobs.EnqueueAsync(
    Jobs.SendEmail,
    new SendEmail("user@example.com", "Welcome", "Hello"),
    new JobEnqueueOptions
    {
        Queue = "mail",
        IdempotencyKey = "welcome:user-42",
        MaxAttempts = 5,
        Timeout = TimeSpan.FromMinutes(2)
    });
```

For this handle:

```csharp
handle.RuntimeMode == QueueRuntimeMode.PostgresManaged;
handle.SupportsStrongStatus == true;
handle.SupportsStrongCancellation == true;
```

Workers claim only when they have free slots. PostgreSQL creates Attempts and leases transactionally with `FOR UPDATE SKIP LOCKED`, and stale Worker Sessions are fenced by SessionId/Epoch/Lease identity.

### Managed ordering

Deployment configuration may select `Parallel`, `KeyOrdered`, or `StrictFifo` for PostgresManaged queues. Those guarantees are implemented by PostgreSQL, not by a RabbitMQ admission/lane layer.

### Managed wake notifications

Immediate PostgresManaged submission no longer inserts one durable `Kj2_Outbox` WorkAvailable row per Run. The control plane first commits the durable Run, then queues a best-effort wake in `ManagedWorkAvailableDispatcher`. That dispatcher coalesces bursts by logical Queue and publishes asynchronously through `IWorkAvailableNotifier`.

Losing an immediate wake cannot lose a Job: PostgreSQL is still the authority and Worker polling remains the correctness path. A lost wake can only add up to the normal polling delay.

Future-dated `NotBefore` jobs and explicit recovery/requeue paths currently retain durable WorkAvailable outbox rows so delayed/recovery wake semantics can be migrated independently from the hot submission path.

## BrokerNative RabbitMQ

BrokerNative bypasses the managed Run/Attempt/Lease path for the normal data plane.

Producer/server registration:

```csharp
builder.Services.AddKubeJobServer();

builder.Services.ConfigureKubeJobQueueRuntimes(options =>
{
    options.Queues["mail"] = new QueueRuntimeRoute
    {
        Mode = QueueRuntimeMode.BrokerNative,
        TransportId = RabbitMqBrokerNativePublisher.Id
    };
});

builder.Services.AddRabbitMqKubeJobBrokerNativeTransport(options =>
{
    options.ConnectionString = rabbitConnectionString;
});
```

Worker registration:

```csharp
builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();

builder.Services.AddKubeJobBrokerNativeWorker(options =>
{
    options.WorkerId = Environment.MachineName;
    options.BuildId = "mailer-2026.08";
    options.MaxConcurrentJobs = 32;
    options.Queues = new List<string> { "mail" };
});

builder.Services.AddRabbitMqKubeJobBrokerNativeConsumer(options =>
{
    options.ConnectionString = rabbitConnectionString;
});
```

The hot path is:

```text
IJobClient
   ↓
RabbitMQ publisher
   ↓
logical Queue
   ↓
competing Worker replicas
   ↓
WorkerExecutionEngine
   ↓
Handler
   ↓
ACK / Retry / DLQ
```

A BrokerNative `JobHandle` identifies the message rather than a PostgreSQL Run:

```csharp
handle.RuntimeMode == QueueRuntimeMode.BrokerNative;
handle.TransportId == "rabbitmq";
handle.SupportsStrongStatus == false;
handle.SupportsStrongCancellation == false;
```

`IJobClient.GetStatusAsync` and `CancelAsync` provide strong managed semantics only. KubeJob does not fabricate a synchronous PostgreSQL projection for BrokerNative messages.

### BrokerNative idempotency

BrokerNative delivery is at-least-once. A message can be redelivered after worker/process/network failure, so external business side effects should be idempotent.

KubeJob does not yet provide a BrokerNative Inbox/deduplication store. Therefore `JobEnqueueOptions.IdempotencyKey` is currently rejected for BrokerNative instead of implying duplicate suppression that does not exist.

## Batch enqueue

`EnqueueBatchAsync` is bounded by `JobRuntimeOptions.MaxSubmissionBatchSize`.

- PostgresManaged batches persist independent Runs atomically in one state-store transaction.
- BrokerNative batches are not atomic. All items are validated before publishing; transports may implement `IMessageTransportBatchPublisher` to amortize durable publish acknowledgements. A failure can still leave a confirmed prefix published.

The RabbitMQ adapter batches publisher confirms so a batch does not require one confirm round trip per message.

## Events

Jobs and Events intentionally have different delivery semantics.

A Job Queue is one competing-consumer pool:

```text
Queue → Worker1 / Worker2 / Worker3
```

An Event Topic fans out to independent Subscriptions:

```text
Topic
 ├─ Subscription A → queue → workers A1..An
 ├─ Subscription B → queue → workers B1..Bn
 └─ Subscription C → queue → workers C1..Cn
```

RabbitMQ Event retries are Subscription-scoped. A failure in one Subscription returns only to that Subscription and does not republish the Topic to already-successful subscribers.

## Schedules

Schedule definitions remain durable in PostgreSQL.

```csharp
await schedules.UpsertCronAsync(
    "daily-report",
    Jobs.GenerateReport,
    new GenerateReport("daily"),
    "0 2 * * *",
    new CronScheduleOptions
    {
        TimeZoneId = "Asia/Tokyo",
        Queue = "reports",
        MisfirePolicy = MisfirePolicy.FireOnce
    });
```

At fire time:

- PostgresManaged creates a durable occurrence Run.
- BrokerNative publishes a deterministic occurrence message and advances the schedule cursor only after publisher confirmation.

Policies that require strong Run state, such as `SkipIfRunning`, remain PostgresManaged capabilities.

## Dashboard

`AddKubeJobDashboard()` provides managed runtime operations such as Run/Attempt history, Worker Sessions, Queue policy and Schedule state.

```csharp
builder.Services.AddKubeJobDashboard(options =>
{
    options.RoutePrefix = "admin/jobs";
    options.AuthorizationPolicy = "KubeJobDashboard";
    options.ShowPayloads = false;
    options.AllowMutatingActions = false;
});
```

The Dashboard hides lease/fencing credentials, is read-only by default, and does not expose RabbitMQ physical exchange/retry/DLX topology as the logical product model.

BrokerNative currently has no strongly consistent per-message lifecycle in the managed Dashboard unless a separate asynchronous projection is implemented in the future.

## PostgreSQL schema

The current managed schema includes:

```text
Kj2_JobRuns
Kj2_JobAttempts
Kj2_WorkerSessions
Kj2_JobSchedules
Kj2_Outbox
```

PostgreSQL is the source of truth only for PostgresManaged execution. BrokerNative queues publish self-contained transport messages and do not use the managed Run/lease/completion path.

`Kj2_Outbox` remains for delayed/recovery managed wake scenarios, but immediate Submit/Batch no longer create one outbox row per Run. Some legacy compatibility columns also remain so the V3 runtime migration is not simultaneously a destructive schema migration.

## Architecture decision

See [ADR 015: V3 Single Authority Runtime Model](./docs/adr/015-v3-single-authority-runtime-model.md) for the current execution-authority decision. Historical BrokerDispatch ADRs remain only as decision history.

## License

MIT
