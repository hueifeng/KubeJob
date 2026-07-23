# 集成说明

## 项目依赖

现有项目已经有：

- Core / Server: .NET 9
- Server: Cronos
- PostgreSQL: Dapper + Npgsql

V2 不要求引入新的第三方运行时依赖。

## 文件归属

- `src/KubeJob.Core/*` -> `src/KubeJob.Core/`
- `src/KubeJob.Server/*` -> `src/KubeJob.Server/`
- `src/KubeJob.Storage.PostgreSQL/*` -> `src/KubeJob.Storage.PostgreSQL/`
- `src/KubeJob.Worker/*` -> `src/KubeJob.Worker/`

`KubeJobServerExtensions.cs`、`KubeJobServerOptions.cs` 是替换/合并文件，不应无脑覆盖未来 main 的修改。

## Server

```csharp
builder.Services.AddKubeJobServer(options =>
    options.UsePostgreSqlRuntimeV2(connectionString));
```

必须映射 Controllers。不要在 LeaseV2 同时注册旧 `JobDispatcherService`、旧 `CronSchedulerService` 或会重置离线节点任务的 `NodeHealthService`。

## Worker

```csharp
builder.Services.AddKubeJobWorkerV2(options =>
{
    options.ServerEndpoint = "https://kubejob-control-plane/";
    options.WorkerId = Environment.GetEnvironmentVariable("POD_NAME")!;
    options.MaxConcurrentJobs = 32;
    options.QueueNames = ["default", "maintenance"];
}, jobAssemblies: typeof(Program).Assembly);
```

Kubernetes 中 WorkerId 应稳定且唯一，例如 StatefulSet Pod 名；每次进程启动的 SessionId 由运行时自动生成。

## 数据库

不要在 Web 进程启动时自动执行 V2 ALTER TABLE。使用正式迁移工具和独立权限账户。迁移前确认历史 timestamp 的时区假设。
