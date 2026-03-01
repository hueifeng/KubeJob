# KubeJob

KubeJob 是一个强大、分布式且可嵌入的 .NET 任务调度框架。它深受 Kubernetes 概念的启发，为您提供了一种现代化的、云原生的任务调度选择。它内置了高质量的 Web 仪表盘，并同时支持单进程嵌入式（Unified）部署和分布式的 Master/Worker 架构部署。

[English Documentation](./README.md)

## 📂 逻辑架构

```text
┌─────────────────────────────────────────────────────────────┐
│                 KubeJob.Server (控制面)                     │
│                                                             │
│  ┌───────────────┐     ┌─────────────────────────────────┐  │
│  │               │     │ 后台核心服务 (Background Svcs)  │  │
│  │ Dashboard UI  │     │ ├─ Cron Scheduler (定时调度)    │  │
│  │               │     │ ├─ Job Dispatcher (任务分发)    │  │
│  └───────┬───────┘     │ └─ Node Health (健康与清理)     │  │
│          │             └─────────────────┬───────────────┘  │
│          v                               │                  │
│  ┌───────────────────────────────────────v───────────────┐  │
│  │                     REST API 端点                     │  │
│  └──────────────────┬────────────────────^───────────────┘  │
└─────────────────────┼────────────────────┼─────────▲────────┘
            (读/写状态)                    │         │
┌─────────────────────v────────────────────┼─┐       │ (1) 注册与心跳
│             可插拔存储引擎               │ │       │ (4) 拉取与上报
│        (In-Memory / PostgreSQL等)        │ │       │
│                                          │ │       │
│ ├─ Kj_JobSpecs (任务规格定义)            │ │       │
│ ├─ Kj_JobRuns (执行队列与日志)           │ │       │
│ └─ Kj_WorkerNodes (集群节点状态)         │ │       │
└──────────────────────────────────────────┘ │       │
                                             │       │
┌────────────────────────────────────────────v───────┴────────┐
│                 KubeJob.Worker (数据面)                     │
│                                                             │
│  ┌──────────────────┐  ┌──────────────────┐                 │
│  │ Worker 节点 A    │  │ Worker 节点 B    │  ...            │
│  │ ├─ 任务执行器    │  │ ├─ 任务执行器    │                 │
│  │ └─ [IKubeJob]    │  │ └─ [IKubeJob]    │                 │
│  └──────────────────┘  └──────────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

KubeJob 的设计严格分离了**调度逻辑**与**执行逻辑**，从而赋予了它极强的横向扩展能力：

- **`KubeJob.Core`**: 基础核心库。包含领域模型、DTO（数据传输对象）、枚举以及核心接口定义（如 `IKubeJob`），供全端共享。
- **`KubeJob.Server` (控制面 Control Plane)**: 
  - **CronScheduler**: 后台调度器，不断轮询数据库，计算 Cron 表达式，筛选出到期任务。
  - **JobDispatcher**: 任务分发器，根据各个 Worker 节点的心跳容量以及 `Node Selectors` 标签匹配规则进行派发。
  - **HistoryCleanup**: 历史清理器，按照配置的上限自动修剪旧数据，防止数据库膨胀。
  - **Dashboard (RCL)**: 提供交互界面的 Razor 类库。
- **`KubeJob.Worker` (数据面 Data Plane)**: 
  - 采用长轮询向 Server 拉取分配给自己的任务，并通过依赖注入 (DI) 容器动态实例化对应的 C# 任务类去执行。

---

## ✨ 核心特性与设计理念

- **云原生调度模型**: KubeJob 深度借鉴了 Kubernetes 的调度模型。它采用 `CronJob` 规格、`Node Selectors`（节点选择器）和 `Execution Models`（单机/广播/分片执行），使其自然地契合现代的微服务和分布式容器化环境。
- **高可用与主节点选举 (Leader Election)**: 您可以部署多个 `KubeJob.Server` 实例而不用担心定时任务重复触发。框架内置了与存储无关的分布式锁提供程序，确保在任意时刻只有一台服务器作为 Leader 进行调度，完美避免了竞态条件，保障了控制面的高可用性。
- **开箱即用的现代化控制面板**: KubeJob 内置了基于 Bootswatch 构建的响应式 Web Dashboard。您可以直接监控节点负载、手工触发任务、动态修改执行策略，大大降低了运维门槛。
- **智能的并发与超时控制**: 原生提供对 `Concurrency Policies`（Allow/Forbid/Replace）的策略支持，并基于 `CancellationToken` 实现了完善的超时熔断与平滑退出 (Graceful Shutdown) 能力。
- **直观的日志与异常可视化**: 在运行历史面板中提供终端风格的深色代码弹窗，直接查看任务的完整异常堆栈 (Stack Traces) 与执行日志，方便快速定位问题。

---

## 🚀 快速开始

### 1. 单体模式 (Unified) 配置 (最简单)

在标准 Web 应用中，您可以将 Server 和 Worker 一起运行在同一个进程中。
只需安装我们的“全家桶”元包：

```bash
dotnet add package KubeJob
```

```csharp
var builder = WebApplication.CreateBuilder(args);

// 1. 注册 KubeJob 服务端 (控制面 + 仪表盘)
// 本地开发推荐使用 InMemory 模式
builder.Services.AddKubeJobServer(opts => opts.UseInMemory());

// 将 Dashboard 挂载到自定义路由前缀
builder.Services.AddKubeJobDashboard(routePrefix: "/admin/jobs");

// 2. 注册 KubeJob 工作节点 (数据面)
builder.Services.AddKubeJobWorker(options => 
{
    // 单体模式下，Worker 指向本机自身地址即可
    options.ServerEndpoint = "http://localhost:5041"; 
    options.MaxConcurrentJobs = 10;
    options.Labels.Add("env", "dev");
});

// 注册您的具体任务类
builder.Services.AddTransient<SampleDataJob>();

var app = builder.Build();

// 3. 初始化存储 Schema (使用持久化数据库时自动建表)
app.InitializeKubeJobDatabase();

app.UseRouting();
app.MapControllers();

// 可选：将网站根目录直接重定向到仪表盘
app.MapGet("/", context => {
    context.Response.Redirect("/admin/jobs");
    return Task.CompletedTask;
});

app.Run();
```

### 2. 编写一个后台任务

创建一个实现了 `IKubeJob` 接口的类，您可以使用 `[KubeJob]` 特性为其定义默认的调度行为。

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
        _logger.LogInformation("任务 {JobId} 正在节点 {NodeId} 上启动", context.RunId, context.WorkerId);
        
        // 模拟任务执行...
        await Task.Delay(2000, token);
        
        // 遇到异常抛出后，框架会自动截获并上报给服务器，您可以在 Dashboard 的弹窗中直接查看堆栈。
        _logger.LogInformation("任务 {JobId} 执行完成.", context.RunId);
    }
}
```

## 📄 许可证

本项目基于 MIT License 开源。
