# RabbitMQ Notification Acceleration

`KubeJob.Transport.RabbitMQ` implements notification-assisted pull. It does not
move durable job ownership into RabbitMQ.

## Control plane

```csharp
builder.Services.AddKubeJobServer(options =>
    options.UsePostgreSql(connectionString));

builder.Services.UseRabbitMqKubeJobNotifications(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
    options.ExchangeName = "kubejob.work-available";
});
```

The transactional Outbox calls `RabbitMqWorkAvailableNotifier`. A direct
exchange routes a small `work-available` message by KubeJob queue name.

## Remote worker

```csharp
builder.Services.AddKubeJobWorkerRuntime(options =>
{
    options.ServerEndpoint = "https://jobs.internal";
    options.WorkerId = Environment.MachineName;
    options.Queues.Add("mail");
    options.MaxConcurrentJobs = 32;
});

builder.Services.AddRabbitMqKubeJobWorkerNotifications(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
    options.ExchangeName = "kubejob.work-available";
});
```

Each worker process creates an exclusive auto-delete notification queue and
binds it to its configured KubeJob queues. A notification releases one bounded
wake signal. The worker then performs the normal HTTP Claim.

## Failure behavior

```text
Run + Outbox transaction commits
        ↓
RabbitMQ publish succeeds or retries
        ↓
Worker receives hint and immediately Claims
        ↓
PostgreSQL atomically creates Attempt + Lease
```

- Duplicate RabbitMQ messages cause extra Claim requests only.
- Missing messages fall back to bounded periodic polling.
- RabbitMQ outage leaves the Outbox retryable and accepted Runs durable.
- A worker that receives a notification but lacks capacity gets no Attempt.
- RabbitMQ delivery tags are never used as KubeJob fencing tokens.
- Completion is still accepted only for the current unexpired Attempt and active
  Worker Session.

## When not to enable it

Do not enable RabbitMQ merely to make KubeJob correct; correctness does not
require it. Enable it when lower idle polling latency or lower empty database
poll volume justifies operating an additional broker.

Unified applications already use an in-process transport and generally do not
need RabbitMQ notifications.
