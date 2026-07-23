# KubeJob V2 使用指南

KubeJob V2 的普通用户只需要理解：任务、Payload、队列、Schedule 和
JobHandle。Run、Attempt、WorkerSession、LeaseToken 与 fencing 属于运行时和运维层。

## 定义任务

```csharp
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

编译时生成器会在当前命名空间生成：

```csharp
Jobs.SendEmail // JobKey<SendEmail>，值为 mail.send
```

业务依赖走构造函数注入。新的执行上下文不会暴露 `IServiceProvider`、数据库、
Repository、LeaseToken 或 fencing token。

## 一体化运行

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKubeJob(
    server => server.UsePostgreSql(connectionString),
    worker =>
    {
        worker.WorkerId = Environment.MachineName;
        worker.MaxConcurrentJobs = 16;
        worker.Queues.Add("mail");
    });

builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();

var app = builder.Build();
app.InitializeKubeJobDatabase();
app.MapControllers();
app.Run();
```

一体化模式使用进程内 transport，不会通过 localhost HTTP 绕一圈，但依然会创建
Run、Attempt 和 Lease，因此和远程 Worker 使用同一套故障语义。

## 提交与幂等

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
```

相同幂等键只有在 JobKey 与 JSON Payload 语义相同时才返回原 Run。相同键但任务或
Payload 不同会抛出 `IdempotencyConflictException`，HTTP API 返回 409。

## 多节点调度

每个 Worker 注册稳定的 WorkerId 和本次进程唯一的 SessionId/Epoch：

```csharp
builder.Services.AddKubeJobWorkerRuntime(options =>
{
    options.ServerEndpoint = "https://jobs.internal";
    options.WorkerId = Environment.MachineName;
    options.MaxConcurrentJobs = 32;
    options.Queues.Add("mail");
    options.BuildId = "mailer-2026.07";
});

builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();
```

多个消费 `mail` 队列的节点会按空闲槽位主动 Pull。服务端会根据已注册
`MaxConcurrency - 活动 Attempt 数量` 重新计算容量，不单独信任 Worker 自报的槽位。
PostgreSQL 使用 `FOR UPDATE SKIP LOCKED` 原子领取，因此两个节点不能获得同一个当前
Attempt。

任务执行后可通过 Attempt 历史看到具体节点：

```text
GET /api/kubejob/jobs/{runId}/attempts
```

同一个 Run 重试时可能由不同节点执行。

## 状态何时变化

```text
提交事务：插入 Run(Pending) + Outbox
Worker Claim 事务：创建 Attempt/Lease，Run → Running
完成事务：Attempt/Run → Succeeded
可重试失败事务：关闭 Attempt，Run → Pending，并写下一条 Outbox
租约过期事务：Attempt → LeaseLost，Run 重排队或 Dead
```

MQ 是否成功发送、消息是否被消费，都不是 Job 状态的事实来源。

## 定时任务

Cron 不再写入 Handler Attribute：

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

Schedule 有独立的持久化记录。多控制面通过可恢复 claim 和版本号协调；推进
`NextFireAt`、创建 occurrence Run 和写 Outbox 在同一事务中完成。

## RabbitMQ 通知加速

控制面：

```csharp
builder.Services.UseRabbitMqKubeJobNotifications(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
});
```

远程 Worker：

```csharp
builder.Services.AddRabbitMqKubeJobWorkerNotifications(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
});
```

RabbitMQ 消息只表示“某个队列可能有工作”。Worker 收到后立即执行普通 Claim。
消息重复只会多一次 Claim；消息丢失则由定期 Pull 兜底。RabbitMQ DeliveryTag 不能替代
KubeJob 的 AttemptId、LeaseToken 或 SessionEpoch。

## 交付保证

KubeJob 明确提供 **at-least-once**。如果 Handler 已完成外部副作用，但进程在上报成功前
崩溃，任务可能再次执行。调用支付、邮件、第三方 API 等外部系统时，应使用业务幂等键或
应用级 Outbox。

旧的非泛型 `IKubeJob` 仍可编译和运行，可以逐个 Handler 迁移，不要求一次性重写。
