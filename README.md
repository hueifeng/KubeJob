# KubeJob

[中文文档](./README_zh.md)

KubeJob is a robust, distributed, and embeddable .NET task scheduling framework inspired by Kubernetes concepts. It offers a modern, cloud-native approach to task scheduling, featuring a high-quality web dashboard and supporting both single-process (Unified) deployments and distributed master/worker architectures.

## 📂 Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                 KubeJob.Server (Control Plane)              │
│                                                             │
│  ┌───────────────┐     ┌─────────────────────────────────┐  │
│  │               │     │ Background Services             │  │
│  │ Dashboard UI  │     │ ├─ Cron Scheduler               │  │
│  │               │     │ ├─ Job Dispatcher               │  │
│  └───────┬───────┘     │ └─ Node Health/History Cleanup  │  │
│          │             └─────────────────┬───────────────┘  │
│          v                               │                  │
│  ┌───────────────────────────────────────v───────────────┐  │
│  │                     REST API Endpoint                 │  │
│  └──────────────────┬────────────────────^───────────────┘  │
└─────────────────────┼────────────────────┼─────────▲────────┘
         (Reads/Writes)                    │         │
┌─────────────────────v────────────────────┼─┐       │ (1) Heartbeat &
│           Pluggable Storage              │ │       │     Register
│        (In-Memory / PostgreSQL)          │ │       │ (4) Poll &
│                                          │ │       │     Report
│ ├─ Kj_JobSpecs (Job Definitions)         │ │       │
│ ├─ Kj_JobRuns (Execution Queue & Logs)   │ │       │
│ └─ Kj_WorkerNodes (Cluster State)        │ │       │
└──────────────────────────────────────────┘ │       │
                                             │       │
┌────────────────────────────────────────────v───────┴────────┐
│                 KubeJob.Worker (Data Plane)                 │
│                                                             │
│  ┌──────────────────┐  ┌──────────────────┐                 │
│  │ Worker Node A    │  │ Worker Node B    │  ...            │
│  │ ├─ Executor      │  │ ├─ Executor      │                 │
│  │ └─ [IKubeJob]    │  │ └─ [IKubeJob]    │                 │
│  └──────────────────┘  └──────────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

KubeJob separates scheduling logic from execution logic, allowing it to scale seamlessly:

- **`KubeJob.Core`**: Shared Domain models, DTOs, Enums, and Interfaces (such as `IKubeJob`).
- **`KubeJob.Server` (Control Plane)**: 
  - **CronScheduler**: Polls the database to compute Cron expressions and identify jobs due for execution.
  - **JobDispatcher**: Handles assigning jobs to available nodes based on capacity and Node Selectors.
  - **HistoryCleanup**: Automatically prunes old execution data based on history limits to prevent database bloat.
  - **Dashboard (RCL)**: The embedded UI layer.
- **`KubeJob.Worker` (Data Plane)**: 
  - Uses long-polling to fetch assigned tasks from the Server, dynamically instantiates your C# class via Dependency Injection (DI), and executes the code while handling cancellation tokens.

---

## ✨ Core Features & Design Philosophy

- **Cloud-Native Scheduling Model**: KubeJob adopts Kubernetes-style scheduling. It utilizes `CronJob` specifications, `Node Selectors`, and `Execution Models` (Standalone/Broadcast/Sharded), making it highly suitable for modern microservices and containerized environments.
- **High Availability (Leader Election)**: Deploy multiple `KubeJob.Server` nodes without the fear of duplicate scheduling. The built-in, storage-agnostic Distributed Lock Provider ensures only one Control Plane node orchestrates tasks at any given time, preventing race conditions and ensuring cluster consistency.
- **Out-of-the-Box Dashboard**: KubeJob includes a built-in Razor dashboard with self-hosted assets, design tokens, and light/dark themes. You can monitor cluster health, manually trigger jobs, and edit specifications on the fly without having to develop your own UI.
- **Smart Concurrency & Resilience**: Native support for `Concurrency Policies` (Allow/Forbid/Replace) combined with `CancellationToken`-based timeouts, automatic retries, and Graceful Shutdowns.
- **Intuitive Log Visualization**: The dashboard includes a readable log dialog to inspect complete exception stack traces and execution logs directly in the UI, accelerating debugging.

---

## 🚀 Getting Started

### 1. Unified Setup (Easiest)

You can run both the Server (Control Plane + Dashboard) and the Worker in the same application. This is ideal for most standard web applications.
Simply install our all-in-one meta-package:

```bash
dotnet add package KubeJob
```

```csharp
var builder = WebApplication.CreateBuilder(args);

// 1. Add KubeJob Server (Control Plane + Dashboard)
builder.Services.AddKubeJobServer(opts => opts.UseInMemory()); 

// Mount the dashboard to a custom route
builder.Services.AddKubeJobDashboard(routePrefix: "/admin/jobs");

// 2. Add KubeJob Worker (Data Plane)
builder.Services.AddKubeJobWorker(options => 
{
    options.ServerEndpoint = "http://localhost:5041"; 
    options.MaxConcurrentJobs = 10;
    options.Labels.Add("env", "dev");
});

// Register your jobs
builder.Services.AddTransient<SampleDataJob>();

var app = builder.Build();

// 3. Initialize Storage Schema (No-op for In-Memory)
app.InitializeKubeJobDatabase();

app.UseRouting();
app.MapControllers();

// Optional: Redirect root to Dashboard
app.MapGet("/", context => {
    context.Response.Redirect("/admin/jobs");
    return Task.CompletedTask;
});

app.Run();
```

### 2. Writing a Job

Create a class that implements `IKubeJob`. You can decorate it with the `[KubeJob]` attribute to define its default scheduling behavior.

```csharp
using KubeJob.Core.Attributes;
using KubeJob.Core.Context;
using KubeJob.Core.Enums;
using KubeJob.Core.Interfaces;

[KubeJob("sample-job-1", Cron = "*/5 * * * *", ExecuteModel = ExecuteModel.Standalone)]
public class SampleDataJob : IKubeJob
{
    private readonly ILogger<SampleDataJob> _logger;

    public SampleDataJob(ILogger<SampleDataJob> logger)
    {
        _logger = logger;
    }

    public async Task ExecuteAsync(KubeJobContext context, CancellationToken token)
    {
        _logger.LogInformation("Job {JobId} starting on Node {NodeId}", context.RunId, context.WorkerId);
        
        // Context contains rich info: context.JobData, context.ShardIndex...
        
        // Simulate work...
        await Task.Delay(2000, token);
        
        // If an exception is thrown, it is automatically logged and sent to the Dashboard's modal view.
        _logger.LogInformation("Job {JobId} completed.", context.RunId);
    }
}
```

## 📄 License

This project is licensed under the MIT License.
