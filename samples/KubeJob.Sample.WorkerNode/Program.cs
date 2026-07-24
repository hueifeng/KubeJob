using KubeJob.Sample.WorkerNode.Jobs;
using KubeJob.Worker.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKubeJobHandler<SampleDataJob, SampleDataPayload>();
builder.Services.AddKubeJobWorker(options =>
{
    options.ServerEndpoint = builder.Configuration["KubeJob:ServerEndpoint"]
        ?? "http://localhost:5041";
    options.WorkerId = Environment.GetEnvironmentVariable("WORKER_ID")
        ?? Environment.MachineName;
    options.MaxConcurrentJobs = 5;
    options.Queues = new List<string> { "default", "samples" };
    options.BuildId = typeof(Program).Assembly.GetName().Version?.ToString() ?? "dev";
    options.Labels["env"] = builder.Environment.EnvironmentName.ToLowerInvariant();
    options.Labels["app"] = "sample-worker";
});

var app = builder.Build();
app.MapGet("/", () => new
{
    service = "KubeJob.Sample.WorkerNode",
    runtime = "typed-pull-worker",
    queues = new[] { "default", "samples" }
});
app.Run();
