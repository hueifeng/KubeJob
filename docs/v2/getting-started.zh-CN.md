# KubeJob 使用指南

KubeJob 现在只保留 V2 运行时。普通使用者主要面对 Typed Handler、Payload、Queue、
Schedule 和 JobHandle；Run、Attempt、WorkerSession、Lease 与 fencing 属于运行时和运维层。

## 1. 定义强类型任务

```csharp
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

编译期生成器会生成稳定的强类型 Key：

```csharp
Jobs.SendEmail // JobKey<SendEmail>，值为 mail.send
```

`[KubeJob]` 只声明稳定 JobKey。业务依赖使用构造函数注入；执行上下文不会暴露
`IServiceProvider`、数据库连接、Repository、LeaseToken 或 fencing token。

## 2. 一体化部署

控制面和 Worker 可以运行在同一进程中，不需要通过 localhost HTTP 调用自身：

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();
builder.Services.AddScoped<IEmailSender, SmtpEmailSender>();

builder.Services.AddKubeJob(
    configureServer: server => server.UsePostgreSql(connectionString),
    configureWorker: worker =>
    {
        worker.WorkerId = Environment.MachineName;
        worker.MaxConcurrentJobs = 16;
        worker.Queues.Add("mail");
        worker.BuildId = "mailer-2026.07";
    });

var app = builder.Build();
app.InitializeKubeJobDatabase();
// 初始化器会应用当前版本化 Schema，并校验数据库契约。
app.MapControllers();
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready")
});
app.Run();
```

一体化模式使用进程内 transport，但仍然创建 Run、Attempt 和 Lease，因此与远程 Worker
保持相同的重试、取消、租约过期和 fencing 语义。

## 3. 提交、查询与幂等

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

相同幂等键只有在 JobKey、JSON Payload 语义以及执行身份（Queue、Priority、ConcurrencyKey、
MaxAttempts、TimeoutSeconds）都相同时才会返回原 Run。相同键用于不同执行身份、不同任务
或不同 Payload 时，会抛出 `IdempotencyConflictException`，HTTP API 返回 409。

取消采用协作式语义：

```csharp
await jobs.CancelAsync(
    handle.JobId,
    "用户取消了导出",
    cancellationToken);
```

## 4. 分布式控制面和 Worker

控制面：

```csharp
builder.Services.AddKubeJobServer(options =>
    options.UsePostgreSql(connectionString));
```

Worker：

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

Worker 只在有空闲槽位时主动 Pull。服务端会根据已注册的 Queue、Capability 和当前活动
Attempt 重新计算容量，不单独信任 Worker 自报。PostgreSQL 使用
`FOR UPDATE SKIP LOCKED` 原子创建唯一的当前 Attempt 和 Lease。

## 5. 状态何时变化

```text
提交事务：插入 Run(Pending) + Outbox
Claim 事务：创建 Attempt/Lease，Run -> Running
成功事务：Attempt/Run -> Succeeded
可重试失败：关闭 Attempt，Run -> Pending，并写下一条 Outbox
租约过期：Attempt -> LeaseLost，Run 重排队或进入 Dead
```

MQ 是否发送成功、消息是否被消费，都不是 Job 状态的事实来源。PostgreSQL 才是权威状态。

## 6. 独立 Schedule

Cron 不再写进 Handler Attribute，而是独立资源：

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

多个控制面通过可过期的 Schedule Claim 和版本号协调。推进 `NextFireAt`、创建 occurrence
Run、写入 Outbox 在同一事务中完成。

## 7. Dashboard

Dashboard 直接使用 V2 的 Run、Attempt、WorkerSession 和 Schedule 模型，包含：

- Overview 与 Queue backlog；
- Run 过滤、分页和详情；
- Attempt 时间线；
- Worker Session、Epoch、容量、Queue、Capability 与 Label；
- Schedule 状态、策略和下一次/上一次触发时间。

生产环境建议绑定现有 ASP.NET Core 授权策略：

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

Dashboard 默认只读，Payload 默认隐藏。只有在路由已被严格保护并且确实需要时，才开启
`ShowPayloads`；只有允许运维人员取消 Run、启停 Schedule 时，才开启
`AllowMutatingActions`。页面永远不会显示 LeaseToken 或 fencing credential。

## 8. RabbitMQ 通知加速

控制面：

```csharp
builder.Services.UseRabbitMqKubeJobNotifications(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
});
```

远程 Worker 可使用 `AddRabbitMqKubeJobWorkerNotifications`。RabbitMQ 消息只表示“某个
Queue 可能有工作”，Worker 收到后仍然执行数据库 Claim。重复消息只会多一次 Claim，消息
丢失则由周期性 Pull 兜底。同一 `ConsumerGroup` 中的 Worker 竞争消费提示，避免每个
Worker 都被同时唤醒并请求数据库。

## 9. 交付保证

KubeJob 明确提供 **at-least-once**。如果 Handler 已完成外部副作用，但进程在上报成功前
崩溃，任务可能再次执行。支付、邮件、第三方 API 等外部调用应使用业务幂等或应用级
Outbox。

旧的非泛型 Handler、Push Dispatcher、JobSpec、WorkerNode、旧表和旧 Dashboard 已从当前
运行时移除，不提供兼容模式。
