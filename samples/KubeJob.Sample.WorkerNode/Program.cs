using KubeJob.Worker.Extensions;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

// Add KubeJob Worker
builder.Services.AddKubeJobWorker(options => 
{
    options.ServerEndpoint = "http://localhost:5041"; // Point to KubeJob.Server
    options.MaxConcurrentJobs = 5;
    
    // Allow overriding WorkerId from env var for running multiple nodes locally
    var envWorkerId = Environment.GetEnvironmentVariable("WORKER_ID");
    if (!string.IsNullOrEmpty(envWorkerId))
    {
        options.WorkerId = envWorkerId;
    }
    
    options.Labels.Add("env", "dev");
    options.Labels.Add("app", "sample");
});

var app = builder.Build();

app.MapGet("/", () => "KubeJob Worker Node is running.");

app.Run();
