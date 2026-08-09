# KubeJob

KubeJob is a .NET runtime for durable jobs and broker-native events. A logical
queue has one clear source of truth, rather than a half-database, half-broker
lifecycle.

| Choose this model | When you need | Authority |
| --- | --- | --- |
| **PostgresManaged** | durable state, idempotency, scheduling, cancellation, retry governance, and operational recovery | PostgreSQL |
| **BrokerNative** | high-throughput delivery and independent event consumers | Your message broker |

Both models provide at-least-once execution. Acknowledgement does not make an
external side effect exactly-once, so handlers that call other systems must be
safe to retry.

## Choose a runtime

### PostgresManaged jobs

Use this for work whose lifecycle is part of the business record: an order
workflow, settlement, report, or a job an operator must inspect and retry.

```text
submit → JobRun → Attempt + lease → handler → durable completion
```

PostgreSQL owns Run, Attempt, lease and worker-fencing state. RabbitMQ can be a
best-effort wake-up signal, but never grants execution ownership.

### BrokerNative jobs and events

Use this for transport-first workloads: integration events, notifications,
logging, or independent subscribers.

```text
publish → broker exchange/topic → queue/subscription → handler → ACK
```

The broker owns delivery, redelivery, retry and dead-letter handling. This path
does not create a managed Run or write leases/completions to PostgreSQL.

BrokerNative rejects managed-only options such as `IdempotencyKey`, `NotBefore`,
`Priority`, `ConcurrencyKey`, continuations and compensations. Choose
PostgresManaged when KubeJob must provide those semantics; otherwise make the
handler idempotent with a business key.

## Quick start: a managed job

Define a typed handler and register it with a stable key:

```csharp
public sealed record SendWelcomeEmail(string Address);

public sealed class SendWelcomeEmailJob : IKubeJob<SendWelcomeEmail>
{
    public ValueTask ExecuteAsync(
        SendWelcomeEmail payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        // Send the email. Honor cancellationToken in real handlers.
        return ValueTask.CompletedTask;
    }
}

var key = new JobKey<SendWelcomeEmail>("mail.send-welcome");
builder.Services.AddKubeJobHandler<SendWelcomeEmailJob, SendWelcomeEmail>(key);
```

For a single-process host, add the server and worker together:

```csharp
builder.Services.AddKubeJob(
    configureServer: server => server.UsePostgreSql(connectionString),
    configureWorker: worker =>
    {
        worker.WorkerId = Environment.MachineName;
        worker.BuildId = "mail-service";
        worker.MaxConcurrentJobs = 16;
        worker.Queues = new List<string> { "mail.send-welcome" };
    });
```

Resolve `IJobClient` to submit work:

```csharp
await jobs.EnqueueAsync(
    key,
    new SendWelcomeEmail("user@example.com"),
    new JobEnqueueOptions
    {
        IdempotencyKey = "welcome:user-42",
        MaxAttempts = 5,
        Timeout = TimeSpan.FromMinutes(2)
    });
```

## Configure a BrokerNative queue

Register a broker transport, route the logical queue to it, then add a native
consumer. RabbitMQ is the included implementation:

```csharp
builder.Services.AddKubeJobServer();
builder.Services.ConfigureKubeJobQueueRuntimes(options =>
{
    options.Queues["order.created"] = new QueueRuntimeRoute
    {
        Mode = QueueRuntimeMode.BrokerNative,
        TransportId = RabbitMqBrokerNativePublisher.Id
    };
});

builder.Services.AddRabbitMqKubeJobBrokerNativeTransport(options =>
    options.ConnectionString = rabbitMqConnectionString);

var orderCreatedKey = new JobKey<OrderCreated>("order.created");
builder.Services.AddKubeJobHandler<OrderCreatedJob, OrderCreated>(orderCreatedKey);
builder.Services.AddKubeJobBrokerNativeWorker(options =>
{
    options.WorkerId = Environment.MachineName;
    options.BuildId = "order-consumer";
    options.Queues = new List<string> { "order.created" };
});
builder.Services.AddRabbitMqKubeJobBrokerNativeConsumer(options =>
    options.ConnectionString = rabbitMqConnectionString);
```

For events, register typed event handlers with explicit subscription names.
Each subscription receives an independent physical queue and retry path.

## Security

KubeJob HTTP and Dashboard endpoints require authorization by default. Configure
named client, worker and dashboard policies—or a host default policy—and call
`UseAuthentication()` and `UseAuthorization()` before mapping controllers.

`AllowAnonymousEndpoints` and `AllowAnonymousAccess` are explicit local
development/test opt-outs. Do not enable them on a public or shared network.

## Development and verification

Start the local PostgreSQL/RabbitMQ stack:

```bash
bash scripts/dev-stack.sh up
```

Run the release test suite:

```bash
dotnet test KubeJob.sln -c Release
```

Set `KUBEJOB_RABBITMQ_TEST_CONNECTION` to include RabbitMQ integration tests;
otherwise those four tests are skipped intentionally.

## Documentation

- [V3 release checklist](docs/v3/release-checklist.md)
- [Runtime model](docs/v3/runtime-model.md)
- [Transport model](docs/v3/transport.md)
- [Event subscriptions](docs/v3/events.md)
- [Benchmarking](docs/v3/benchmarking.md)
- [Historical V2 material](docs/v2/README.md)

## License

MIT
