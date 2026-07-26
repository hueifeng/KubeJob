# RabbitMQ Notification Acceleration

`KubeJob.Transport.RabbitMQ` implements notification-assisted pull. It does not
move durable job ownership into RabbitMQ.

## Business-message ingress

Business messages and wake-up notifications use different RabbitMQ topologies.
Register the business-message Adapter when RabbitMQ is the source of job
submissions:

```csharp
builder.Services.AddRabbitMqKubeJobIngress(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
    options.ExchangeName = "kubejob.job-ingress";
    options.QueueName = "mailer-ingress";
    options.RoutingKey = "mail.#";
    options.Source = "rabbitmq.mailer";
    options.DeadLetterExchangeName = "kubejob.job-ingress.dlx";
    options.DeadLetterRoutingKey = "dead";
});
```

The JSON body is a `RabbitMqJobIngressEnvelope` containing `MessageId`,
`JobKey`, `PayloadJson`, `Queue`, and retry/availability fields. The AMQP
`MessageId` property takes precedence when present. The Adapter uses
`Source:MessageId` as the KubeJob idempotency key, ACKs only after durable
submission, rejects permanent invalid/conflicting messages, and requeues
transient failures.

The real broker test is kept separate from the ordinary unit suite. With a
RabbitMQ broker available, run it explicitly:

```bash
KUBEJOB_RABBITMQ_TEST_CONNECTION=amqp://guest:guest@localhost:5672/ \
  dotnet test tests/KubeJob.RabbitMqIntegrationTests/KubeJob.RabbitMqIntegrationTests.csproj
```

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
builder.Services.AddKubeJobWorker(options =>
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
    options.ConsumerGroup = "mail-workers";
});
```

For each configured KubeJob Queue, workers in the same `ConsumerGroup` share an
auto-delete RabbitMQ queue and compete for wake-up messages. One hint therefore
wakes one worker pool member instead of every worker. Different groups receive
independent copies and should be used only for genuinely independent pools.

The listener manually acknowledges a hint after it has pulsed the bounded local
claim trigger. The worker then performs the normal HTTP Claim. Publishers use
RabbitMQ publisher confirms before the Outbox record is marked published.

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
- Multiple workers in one consumer group compete for each wake-up hint.
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
