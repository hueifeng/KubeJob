# KubeJob V2 Getting Started

KubeJob V2 separates the public job API from the distributed runtime. Existing
legacy jobs remain supported while applications migrate one handler at a time.

## 1. Define a payload and typed handler

```csharp
using KubeJob.Core.Attributes;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;

public sealed record SendEmail(
    string To,
    string Subject,
    string Body);

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

The source generator emits a stable, strongly typed key in the handler's
namespace:

```csharp
Jobs.SendEmail // JobKey<SendEmail>, value "mail.send"
```

The execution context is read-only and does not expose `IServiceProvider`, a
database connection, repository, lease token, or fencing token. Business
dependencies use constructor injection.

## 2. Unified application

The control plane and worker can share one process without localhost HTTP:

```csharp
using KubeJob;
using KubeJob.Server.Extensions;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Worker.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKubeJob(
    server => server.UsePostgreSql(
        builder.Configuration.GetConnectionString("KubeJob")!),
    worker =>
    {
        worker.WorkerId = Environment.MachineName;
        worker.MaxConcurrentJobs = 16;
        worker.Queues.Add("mail");
        worker.BuildId = "mailer-2026.07";
    });

builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>("mail.send");
builder.Services.AddScoped<IEmailSender, SmtpEmailSender>();

var app = builder.Build();
app.InitializeKubeJobDatabase();
app.MapControllers();
app.Run();
```

`AddKubeJob` selects the in-process worker transport. Submission, claim,
Attempt, lease, retry, cancellation, and fencing semantics remain identical to
a distributed deployment.

## 3. Submit a job

```csharp
public sealed class UsersController : ControllerBase
{
    private readonly IJobClient _jobs;

    public UsersController(IJobClient jobs)
    {
        _jobs = jobs;
    }

    [HttpPost("{userId}/welcome")]
    public async Task<IActionResult> Welcome(
        string userId,
        CancellationToken cancellationToken)
    {
        var handle = await _jobs.EnqueueAsync(
            Jobs.SendEmail,
            new SendEmail(
                "user@example.com",
                "Welcome",
                "Welcome to the service."),
            new JobEnqueueOptions
            {
                Queue = "mail",
                IdempotencyKey = $"welcome:{userId}",
                MaxAttempts = 5,
                Timeout = TimeSpan.FromMinutes(2)
            },
            cancellationToken);

        return Accepted(new { handle.JobId });
    }
}
```

An idempotency key may be reused only for the same JobKey and semantically equal
JSON payload. A different job or payload produces
`IdempotencyConflictException`; the HTTP API returns `409 Conflict`.

## 4. Query and cancel

```csharp
var status = await jobs.GetStatusAsync(handle.JobId, cancellationToken);

if (status?.Phase == JobPhase.Running)
{
    Console.WriteLine($"Worker: {status.WorkerId}");
    Console.WriteLine($"Attempt: {status.Attempt}");
}

await jobs.CancelAsync(
    handle.JobId,
    reason: "The user canceled the export",
    cancellationToken);
```

The control-plane API also exposes:

```text
GET  /api/kubejob/jobs/{runId}
GET  /api/kubejob/jobs/{runId}/attempts
POST /api/kubejob/jobs/{runId}/cancel
```

Attempt history is the authoritative answer to which Worker Session executed a
job. A retry may run on a different worker.

## 5. Independent control plane and worker

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

The worker pulls only when it has free slots. The server verifies capacity from
active Attempts and does not trust a worker's reported slot count by itself.

## 6. Cron schedules

Schedules are independent resources; cron configuration is not stored on the
handler attribute.

```csharp
public sealed record GenerateReport(string Kind);

var schedules = serviceProvider.GetRequiredService<IJobScheduleClient>();

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
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.SkipIfRunning,
        MaxAttempts = 3,
        Timeout = TimeSpan.FromMinutes(30)
    });
```

The schedule reconciler uses a recoverable claim lease. In PostgreSQL, advancing
`NextFireAt`, creating the logical Run, and writing the Outbox event occur in one
transaction. Every occurrence has a deterministic identity.

## 7. Delivery guarantees

KubeJob provides **at-least-once execution**. A worker process can finish an
external side effect and fail before reporting success, so handlers that call
external systems should use domain idempotency or an application outbox where
necessary.

KubeJob prevents stale workers from changing current state by validating all of
these values:

```text
RunId
AttemptId
AttemptNumber
WorkerId
SessionId
SessionEpoch
LeaseToken
CurrentAttemptId
LeaseExpiresAt
```

A lease-expired or stale-session completion is rejected even if the lease
reconciler has not processed it yet.

## 8. MQ integration boundary

PostgreSQL remains the source of truth. Submission stores the Run and Outbox in
one transaction. `IWorkAvailableNotifier` is the extension point for RabbitMQ,
NATS JetStream, Azure Service Bus, or another notification system.

An MQ notification is only a wake-up hint. Workers still claim from the state
store, and duplicate or missing notifications do not create a second valid
Attempt. The Outbox publishing claim is recoverable after publisher crashes.

## 9. Migration from the legacy runtime

1. Keep existing non-generic `IKubeJob` handlers running.
2. Convert one handler to `IKubeJob<TPayload>`.
3. Register it with `AddKubeJobHandler<TJob, TPayload>`.
4. Submit through `IJobClient` and the generated `Jobs.*` key.
5. Enable the V2 worker runtime for the queues migrated to typed handlers.
6. Move cron definitions from `[KubeJob]` attributes to `IJobScheduleClient`.
7. Retire legacy JobSpecs only after their active runs and schedules are drained.

The generator intentionally ignores legacy non-generic handlers, so migration
does not require an all-at-once rewrite.
