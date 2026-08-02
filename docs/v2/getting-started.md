# KubeJob Getting Started

KubeJob is a V2-only typed distributed job runtime. Applications define typed
handlers, submit logical Runs, and optionally create independent cron Schedules.
Workers, Attempts, leases, fencing, and Outbox delivery remain runtime concerns.

## 1. Define a typed handler

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

    public SendEmailJob(IEmailSender sender) => _sender = sender;

    public ValueTask ExecuteAsync(
        SendEmail payload,
        JobExecutionContext context,
        CancellationToken cancellationToken) =>
        _sender.SendAsync(payload, cancellationToken);
}
```

The source generator emits a stable strongly typed key:

```csharp
Jobs.SendEmail // JobKey<SendEmail>, value "mail.send"
```

`[KubeJob]` declares only the stable handler key. Handler dependencies use
constructor injection. `JobExecutionContext` exposes a scoped `IServiceProvider`
for middleware and handler resolution, but never a storage connection,
repository, lease token, or fencing token.

## 2. Unified application

The control plane and worker can share one process without localhost HTTP:

```csharp
using KubeJob;
using KubeJob.Server.Extensions;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Worker.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();
builder.Services.AddScoped<IEmailSender, SmtpEmailSender>();

builder.Services.AddKubeJob(
    configureServer: server => server.UsePostgreSql(
        builder.Configuration.GetConnectionString("KubeJob")!),
    configureWorker: worker =>
    {
        worker.WorkerId = Environment.MachineName;
        worker.MaxConcurrentJobs = 16;
        worker.Queues.Add("mail");
        worker.BuildId = "mailer-2026.07";
    });

var app = builder.Build();
app.InitializeKubeJobDatabase();
// The initializer applies the current versioned schema and validates the contract.
app.MapControllers();
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready")
});
app.Run();
```

Unified hosting uses the in-process transport but preserves the same Run,
Attempt, lease, retry, cancellation, and fencing semantics as remote workers.

## 3. Submit and query a job

```csharp
var handle = await jobs.EnqueueAsync(
    Jobs.SendEmail,
    new SendEmail("user@example.com", "Welcome", "Hello"),
    new JobEnqueueOptions
    {
        Queue = "mail",
        IdempotencyKey = $"welcome:{userId}",
        MaxAttempts = 5,
        Timeout = TimeSpan.FromMinutes(2)
    },
    cancellationToken);

var status = await jobs.GetStatusAsync(handle.JobId, cancellationToken);
```

An idempotency key may be reused only for the same JobKey, semantically equal
JSON payload, and execution identity (Queue, Priority, ConcurrencyKey,
MaxAttempts, and TimeoutSeconds). Reusing it with another execution identity,
job, or payload throws `IdempotencyConflictException`; the HTTP API returns
`409 Conflict`.

Cancellation is cooperative:

```csharp
await jobs.CancelAsync(
    handle.JobId,
    "The user canceled the export",
    cancellationToken);
```

## 4. Distributed control plane and workers

Control plane:

```csharp
builder.Services.AddKubeJobServer(options =>
    options.UsePostgreSql(connectionString));
```

Worker:

```csharp
builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();
builder.Services.AddKubeJobWorker(options =>
{
    options.ServerEndpoint = "https://jobs.internal";
    options.WorkerId = Environment.MachineName;
    options.MaxConcurrentJobs = 32;
    options.Queues.Add("mail");
    options.BuildId = "mailer-2026.07";
});
```

Workers pull only when they have free slots. The server validates each claim
against the registered Worker Session queues and capabilities and recalculates
capacity from active Attempts. PostgreSQL uses `FOR UPDATE SKIP LOCKED` to
create one current Attempt and lease atomically.

## 5. State transitions

```text
Submission transaction: Run(Pending) + Outbox
Claim transaction: create Attempt/lease, Run -> Running
Success transaction: Attempt/Run -> Succeeded
Retryable failure: close Attempt, Run -> Pending, write another Outbox entry
Lease expiry: Attempt -> LeaseLost, Run -> Pending or Dead
```

MQ publication and consumption are not job states. PostgreSQL remains the source
of truth.

## 6. Independent schedules

Cron configuration belongs to a Schedule resource, not the handler attribute:

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
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.SkipIfRunning,
        MaxAttempts = 3,
        Timeout = TimeSpan.FromMinutes(30)
    });
```

Multiple control-plane replicas use expiring Schedule claims and optimistic
versions. Advancing `NextFireAt`, creating the occurrence Run, and writing its
Outbox entry happen in one transaction.

## 7. Dashboard

The embedded Dashboard is V2-native and displays Overview, queue backlog, Runs,
Attempt timelines, Worker Sessions, and Schedules.

```csharp
builder.Services.AddAuthorization(options =>
{
    options.AddPolicy("KubeJobDashboard", policy =>
        policy.RequireRole("KubeJobOperator"));
});

builder.Services.AddKubeJobDashboard(options =>
{
    options.RoutePrefix = "admin/jobs";
    options.AuthorizationPolicy = "KubeJobDashboard";
    options.ShowPayloads = false;
    options.AllowMutatingActions = false;
});

var app = builder.Build();
app.UseAuthentication();
app.UseAuthorization();
app.MapControllers();
```

The Dashboard is read-only by default. Payloads are hidden by default. Set
`AllowMutatingActions` only when operators should be able to cancel Runs and
enable or disable Schedules. Lease and fencing credentials are never rendered.

## 8. RabbitMQ notification acceleration

```csharp
builder.Services.UseRabbitMqKubeJobNotifications(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
});
```

Remote workers can add `AddRabbitMqKubeJobWorkerNotifications` with the same
broker settings. Notifications are only queue-specific wake-up hints. Workers
still claim from PostgreSQL, so duplicate or missing messages cannot create
another valid Attempt. Workers with the same `ConsumerGroup` compete for each
hint, avoiding a claim storm across every worker.

If RabbitMQ is also the business-message source, register its ingress queue
separately:

```csharp
builder.Services.AddRabbitMqKubeJobIngress(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
    options.QueueName = "mailer-ingress";
    options.RoutingKey = "mail.#";
    options.Source = "rabbitmq.mailer";
    options.DeadLetterExchangeName = "kubejob.job-ingress.dlx";
    options.DeadLetterRoutingKey = "dead";
});
```

Ingress ACK happens after the durable Run hand-off. Invalid messages are
rejected for dead-letter handling; transient persistence or connectivity
failures are requeued. This business-message path is independent from the
notification exchange.

## 9. Delivery guarantee

KubeJob explicitly provides **at-least-once execution**. A worker can finish an
external side effect and crash before reporting success. Handlers that call
payment, email, or other external systems should use domain idempotency or an
application Outbox.

The previous non-generic handler API, push dispatcher, JobSpec model, WorkerNode
model, legacy tables, and legacy Dashboard are not part of this runtime.
