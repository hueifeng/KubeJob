# KubeJob

[中文指南](./docs/v2/getting-started.zh-CN.md) · [Getting Started](./docs/v2/getting-started.md) · [Architecture](./docs/v2/architecture.md)

KubeJob is a typed, embeddable, distributed background-job runtime for .NET.
It uses logical Runs, physical Attempts, pull-based workers, expiring leases,
worker-session fencing, PostgreSQL transactions, an Outbox, and independent
cron Schedule resources.

KubeJob provides **at-least-once execution**. It does not claim exactly-once
external side effects.

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

The source generator creates a strongly typed key such as `Jobs.SendEmail`.
`JobExecutionContext` is read-only and does not expose a service locator,
repository, lease token, or fencing token.

## Register and enqueue

```csharp
builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();

await jobs.EnqueueAsync(
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

`[KubeJob]` declares only the stable handler key. Queue, priority, retry,
timeout, idempotency, concurrency, scheduling, placement, batching, sharding,
and broadcast behavior belong to submissions or dedicated resources.

## Unified deployment

The control plane and worker can share one process without localhost HTTP:

```csharp
builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();
builder.Services.AddKubeJob(
    configureServer: server => server.UsePostgreSql(connectionString),
    configureWorker: worker =>
    {
        worker.WorkerId = Environment.MachineName;
        worker.MaxConcurrentJobs = 16;
        worker.Queues = new List<string> { "mail" };
        worker.BuildId = "mailer-2026.07";
    });

builder.Services.AddKubeJobDashboard("admin/jobs");

var app = builder.Build();
app.InitializeKubeJobDatabase();
app.MapControllers();
app.Run();
```

The in-process transport preserves the same Attempt, lease, retry,
cancellation, and fencing semantics as distributed deployment.

## Distributed deployment

Control plane:

```csharp
builder.Services.AddKubeJobServer(options =>
    options.UsePostgreSql(connectionString));
builder.Services.AddKubeJobDashboard();
```

Worker:

```csharp
builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();
builder.Services.AddKubeJobWorker(options =>
{
    options.ServerEndpoint = "https://jobs.internal";
    options.WorkerId = Environment.MachineName;
    options.MaxConcurrentJobs = 32;
    options.Queues = new List<string> { "mail" };
    options.BuildId = "mailer-2026.07";
});
```

Workers request work only when they have free slots. PostgreSQL atomically
creates an Attempt and lease with `FOR UPDATE SKIP LOCKED`. The server derives
capacity from active Attempts and validates claims against the queues and
capabilities registered by the Worker Session.

## Independent schedules

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

Multiple control-plane replicas reconcile schedules through expiring claims
and optimistic versions. Cursor advancement, Run creation, and Outbox creation
occur in one PostgreSQL transaction.

## Runtime model

```text
JobSchedule ──creates──> JobRun ──contains──> JobAttempt

Worker ──starts──> WorkerSession ──temporarily owns──> JobAttempt
```

A retry or reassignment creates another Attempt under the same logical Run.
Completion is accepted only from the current unexpired Attempt and active
Worker Session. Stale workers cannot overwrite newer sessions.

## Dashboard

`AddKubeJobDashboard()` provides V2-native operational pages:

- runtime overview and Outbox backlog;
- logical Run filtering and pagination;
- Run detail with a complete Attempt timeline;
- Worker Session state, epoch, capacity, queues, capabilities, labels, and heartbeat;
- independent Schedule state, policies, next/last fire time, and enable/disable actions.

The Dashboard deliberately does not expose lease or fencing credentials. Run
payloads may contain application data, so production hosts should protect the
Dashboard route with their normal authentication and authorization policy.

## HTTP diagnostics

```text
GET  /api/kubejob/jobs/{runId}
GET  /api/kubejob/jobs/{runId}/attempts
POST /api/kubejob/jobs/{runId}/cancel

PUT    /api/kubejob/schedules/{scheduleId}
GET    /api/kubejob/schedules/{scheduleId}
POST   /api/kubejob/schedules/{scheduleId}/enabled
DELETE /api/kubejob/schedules/{scheduleId}
```

Attempt history is the authoritative answer to which Worker Session executed a
job; retries may move between nodes.

## PostgreSQL schema

```text
Kj2_JobRuns
Kj2_JobAttempts
Kj2_WorkerSessions
Kj2_JobSchedules
Kj2_Outbox
```

PostgreSQL is the source of truth. Optional MQ integration publishes only
queue-specific wake-up hints from the transactional Outbox; duplicate or
missing notifications cannot grant ownership.

## License

MIT
