# KubeJob

[中文说明](./README_zh.md) · [V2 Getting Started](./docs/v2/getting-started.md) · [V2 Architecture](./docs/v2/architecture.md)

KubeJob is an embeddable and distributed .NET background-job runtime. The V2
architecture adds strongly typed payloads, logical Run/physical Attempt
separation, pull scheduling, worker-session fencing, expiring leases,
transactional Outbox delivery, and independent cron schedules.

The existing legacy runtime remains available during migration.

## Typed job API

```csharp
public sealed record SendEmail(string To, string Subject, string Body);

[KubeJob("mail.send")]
public sealed class SendEmailJob : IKubeJob<SendEmail>
{
    private readonly IEmailSender _sender;

    public SendEmailJob(IEmailSender sender)
    {
        _sender = sender;
    }

    public ValueTask ExecuteAsync(
        SendEmail payload,
        JobExecutionContext context,
        CancellationToken cancellationToken) =>
        _sender.SendAsync(payload, cancellationToken);
}
```

The included incremental source generator creates a stable typed key:

```csharp
await jobs.EnqueueAsync(
    Jobs.SendEmail,
    new SendEmail("user@example.com", "Welcome", "Hello"),
    new JobEnqueueOptions
    {
        Queue = "mail",
        IdempotencyKey = "welcome:user-42",
        MaxAttempts = 5
    });
```

Handlers use constructor injection. `JobExecutionContext` does not expose a
service locator, storage repository, HTTP client, lease token, or fencing token.

## Unified deployment

Control plane and worker can run in one process without localhost HTTP:

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKubeJob(
    server => server.UsePostgreSql(
        builder.Configuration.GetConnectionString("KubeJob")!),
    worker =>
    {
        worker.WorkerId = Environment.MachineName;
        worker.MaxConcurrentJobs = 16;
        worker.Queues.Add("mail");
    });

builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>("mail.send");

var app = builder.Build();
app.InitializeKubeJobDatabase();
app.MapControllers();
app.Run();
```

The in-process transport preserves the same Attempt, lease, retry, cancellation,
and fencing semantics as a remote worker.

## Distributed deployment

Control plane:

```csharp
builder.Services.AddKubeJobServer(options =>
    options.UsePostgreSql(connectionString));
```

Worker:

```csharp
builder.Services.AddKubeJobWorkerRuntime(options =>
{
    options.ServerEndpoint = "https://jobs.internal";
    options.WorkerId = Environment.MachineName;
    options.MaxConcurrentJobs = 32;
    options.Queues.Add("mail");
    options.BuildId = "mailer-2026.07";
});

builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>("mail.send");
```

Workers pull only when they have free slots. PostgreSQL atomically creates a
physical Attempt and lease using `FOR UPDATE SKIP LOCKED`. The server recalculates
capacity from active Attempts and does not rely on worker-reported capacity
alone.

## Cron schedules

Schedules are independent from handler attributes:

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
        MisfirePolicy = MisfirePolicy.FireOnce,
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.SkipIfRunning
    });
```

Multiple control-plane replicas reconcile schedules through recoverable claims
and transactional version checks. Advancing the schedule, creating the Run, and
writing the Outbox notification happen atomically.

## Runtime model

```text
JobSchedule ──creates──> JobRun ──contains──> JobAttempt

Worker ──starts──> WorkerSession ──temporarily owns──> JobAttempt
```

A retry creates a new Attempt for the same logical Run. Attempt history records
the WorkerId, SessionId, SessionEpoch, lease outcome, and completion result.

Completion is accepted only from the current unexpired Attempt and active Worker
Session. A stale worker cannot overwrite a newer Session even when it later
recovers network connectivity.

## Delivery guarantee

KubeJob provides **at-least-once execution**. It does not claim exactly-once
external side effects. Handlers that call external systems should use domain
idempotency or an application Outbox where required.

PostgreSQL is the state source. MQ integration uses `IWorkAvailableNotifier` as
an asynchronous wake-up hint; duplicate or missing notifications cannot create a
second valid Attempt. Workers always claim against durable state.

## Status and diagnostics

```text
GET  /api/kubejob/jobs/{runId}
GET  /api/kubejob/jobs/{runId}/attempts
POST /api/kubejob/jobs/{runId}/cancel

PUT    /api/kubejob/schedules/{scheduleId}
GET    /api/kubejob/schedules/{scheduleId}
POST   /api/kubejob/schedules/{scheduleId}/enabled
DELETE /api/kubejob/schedules/{scheduleId}
```

The Attempt list is the authoritative answer to which node executed a job; a
retry may move to another Worker Session.

## Storage

The PostgreSQL V2 schema uses separate tables for:

```text
Kj2_JobRuns
Kj2_JobAttempts
Kj2_WorkerSessions
Kj2_JobSchedules
Kj2_Outbox
```

Legacy `Kj_*` tables are retained during migration.

## Migration

Legacy non-generic `IKubeJob` handlers continue to compile. Convert handlers one
at a time to `IKubeJob<TPayload>`, register them with
`AddKubeJobHandler<TJob, TPayload>`, and move cron configuration into
`IJobScheduleClient`.

See [V2 Getting Started](./docs/v2/getting-started.md) for the complete migration
sequence and operational semantics.

## License

MIT
