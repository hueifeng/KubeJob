using KubeJob;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Sample.RemoteWorker.Jobs;
using KubeJob.Sample.Unified.Jobs;
using KubeJob.Server.Extensions;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;

var builder = WebApplication.CreateBuilder(args);
var postgresConnectionString = builder.Configuration.GetConnectionString("KubeJob");
var rabbitMqConnectionString = builder.Configuration.GetConnectionString("RabbitMQ");
var workerId = Environment.GetEnvironmentVariable("KUBEJOB_WORKER_ID") ?? "unified-sample";
var maxConcurrentJobs = ParsePositiveInt("KUBEJOB_MAX_CONCURRENT_JOBS", 100);
var prefetchCount = ParsePositiveInt("KUBEJOB_PREFETCH_COUNT", maxConcurrentJobs);
var databasePoolSize = ParsePositiveInt("KUBEJOB_DB_POOL_SIZE", 64);
var databaseConcurrency = ParsePositiveInt("KUBEJOB_DB_CONCURRENCY", databasePoolSize);

builder.Services.AddKubeJobHandler<SampleDataJob, SampleDataPayload>();
builder.Services.AddKubeJobHandler<DashboardDemoJob, DashboardDemoPayload>();
builder.Services.AddKubeJob(
    configureServer: options =>
    {
        if (string.IsNullOrWhiteSpace(postgresConnectionString))
        {
            options.UseInMemory();
        }
        else
        {
            options.UsePostgreSql(
                postgresConnectionString,
                postgres =>
                {
                    postgres.BusinessPoolSize = databasePoolSize;
                    postgres.MaximumConcurrentOperations = databaseConcurrency;
                });
        }
    },
    configureWorker: options =>
    {
        options.WorkerId = workerId;
        options.MaxConcurrentJobs = maxConcurrentJobs;
        options.Queues = new List<string> { "sample.data", "sample.dashboard-demo" };
        options.BuildId = typeof(Program).Assembly.GetName().Version?.ToString() ?? "dev";
        options.Labels["env"] = builder.Environment.EnvironmentName.ToLowerInvariant();
        options.Labels["app"] = "unified-sample";
        // The worker session must register under the same consumer group the
        // RabbitMQ transport is provisioned for.
        options.ConsumerGroup = "unified-sample";
    });
builder.Services.AddKubeJobDashboard(options =>
{
    options.RoutePrefix = "admin/jobs";
    // This sample is local-development only. Enabling mutations makes the
    // long-running demo job useful for exercising cooperative cancellation.
    options.AllowMutatingActions = true;
});

if (!string.IsNullOrWhiteSpace(rabbitMqConnectionString))
{
    builder.Services.ConfigureKubeJobQueueRouting(options =>
    {
        // The sample keeps routing deployment-owned: business callers still
        // submit only a logical queue and cannot select RabbitMQ per Run.
        // Each queue the worker serves gets one definition; the consumer
        // group must match the transport group configured below.
        options.Queues["sample.data"] = new KubeJob.Server.Runtime.QueueDefinition
        {
            ConsumerGroup = "unified-sample"
        };
        options.Queues["sample.dashboard-demo"] = new KubeJob.Server.Runtime.QueueDefinition
        {
            ConsumerGroup = "unified-sample"
        };
    });

    void ConfigureRabbitMq(RabbitMqExecutionOptions options)
    {
        options.ConnectionString = rabbitMqConnectionString;
        options.ConsumerGroup = "unified-sample";
        options.PrefetchCount = (ushort)Math.Min(prefetchCount, ushort.MaxValue);
    }

    builder.Services.UseRabbitMqKubeJobExecutionDispatcher(ConfigureRabbitMq);
    builder.Services.AddRabbitMqKubeJobExecutionConsumer(ConfigureRabbitMq);
}

var app = builder.Build();
if (!string.IsNullOrWhiteSpace(postgresConnectionString))
{
    app.InitializeKubeJobDatabase();
}

app.UseStaticFiles();
app.UseRouting();
app.MapControllers();
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready")
});

var dashboardDemoJob = new JobKey<DashboardDemoPayload>("sample.dashboard-demo");
app.MapPost("/demo/scenarios", async (IJobClient jobs, CancellationToken cancellationToken) =>
{
    var batchId = Guid.NewGuid().ToString("N");
    var scenarios = new (string Name, DashboardDemoPayload Payload, JobEnqueueOptions Options, string Expected)[]
    {
        (
            "success",
            new DashboardDemoPayload("success", DelayMilliseconds: 250),
            new JobEnqueueOptions
            {
                Queue = "samples",
                MaxAttempts = 1,
                Timeout = TimeSpan.FromSeconds(10),
                IdempotencyKey = $"dashboard-demo:{batchId}:success"
            },
            "Succeeded after one Attempt"),
        (
            "retry-then-success",
            new DashboardDemoPayload("retry-then-success", DelayMilliseconds: 250, FailUntilAttempt: 1),
            new JobEnqueueOptions
            {
                Queue = "samples",
                MaxAttempts = 3,
                Timeout = TimeSpan.FromSeconds(10),
                IdempotencyKey = $"dashboard-demo:{batchId}:retry-success"
            },
            "First Attempt fails; second Attempt succeeds"),
        (
            "exhausted-retries",
            new DashboardDemoPayload("always-fail"),
            new JobEnqueueOptions
            {
                Queue = "samples",
                MaxAttempts = 2,
                Timeout = TimeSpan.FromSeconds(10),
                IdempotencyKey = $"dashboard-demo:{batchId}:dead"
            },
            "Retryable failures exhaust MaxAttempts and become Dead"),
        (
            "permanent-failure",
            new DashboardDemoPayload("permanent-failure"),
            new JobEnqueueOptions
            {
                Queue = "samples",
                MaxAttempts = 3,
                Timeout = TimeSpan.FromSeconds(10),
                IdempotencyKey = $"dashboard-demo:{batchId}:permanent"
            },
            "Payload validation failure becomes a permanent failure without retry"),
        (
            "timeout",
            new DashboardDemoPayload("timeout", DelayMilliseconds: 5_000),
            new JobEnqueueOptions
            {
                Queue = "samples",
                MaxAttempts = 2,
                Timeout = TimeSpan.FromSeconds(1),
                IdempotencyKey = $"dashboard-demo:{batchId}:timeout"
            },
            "Both Attempts time out and the Run becomes Dead"),
        (
            "cancel-me",
            new DashboardDemoPayload("long-running", DelayMilliseconds: 60_000),
            new JobEnqueueOptions
            {
                Queue = "samples",
                MaxAttempts = 1,
                Timeout = TimeSpan.FromSeconds(90),
                IdempotencyKey = $"dashboard-demo:{batchId}:cancel"
            },
            "Use the Dashboard cancellation action while the job is running")
    };

    var submitted = new List<object>(scenarios.Length);
    foreach (var scenario in scenarios)
    {
        var handle = await jobs.EnqueueAsync(
            dashboardDemoJob,
            scenario.Payload,
            scenario.Options,
            cancellationToken);
        submitted.Add(new
        {
            scenario = scenario.Name,
            expected = scenario.Expected,
            runId = handle.JobId,
            dashboard = $"/admin/jobs/runs/{handle.JobId}"
        });
    }

    return Results.Accepted("/admin/jobs", new
    {
        batchId,
        dashboard = "/admin/jobs",
        failures = "/admin/jobs/failures",
        jobs = submitted
    });
});

app.MapGet("/", context =>
{
    context.Response.Redirect("/admin/jobs");
    return Task.CompletedTask;
});
app.Run();

static int ParsePositiveInt(string name, int fallback)
{
    var value = Environment.GetEnvironmentVariable(name);
    return int.TryParse(value, out var parsed) && parsed > 0
        ? parsed
        : fallback;
}
