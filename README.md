# KubeJob

KubeJob is a .NET library for work that should outlive the request that created
it. It gives you typed handlers, retries, leases, cancellation, and an
operator-facing dashboard for database-backed jobs. When a message broker is
the right place to own delivery, the same handler model can consume
broker-native jobs and events.

[Quick start](#quick-start) ·
[Pick a runtime](#pick-a-runtime) ·
[Local development](docs/v3/local-development.md) ·
[Benchmarking](docs/v3/benchmarking.md) ·
[Samples](#samples) ·
[Documentation](docs/README.md) ·
[中文入口](README_zh.md)

## Quick start

The repository includes a PostgreSQL and RabbitMQ development stack. With
Podman installed, start it with:

```bash
KUBEJOB_CONTAINER_ENGINE=podman bash scripts/dev-stack.sh up
```

The script also works with Docker Compose and selects an available engine when
`KUBEJOB_CONTAINER_ENGINE` is not set. You need the .NET 10 SDK to build and
test the repository.

Run the sample host in another terminal:

```bash
bash scripts/run-unified-sample.sh
```

Open the dashboard at <http://localhost:5041/admin/jobs>. To create a few
successful, retried, timed-out, and cancelled jobs, run:

```bash
bash scripts/seed-dashboard-demo.sh
```

See [Local development](docs/v3/local-development.md) for Windows commands,
service credentials, integration tests, and cleanup.

## Pick a runtime

KubeJob has two deliberately separate ways to execute work. Pick one per
queue; a queue is not partly managed by PostgreSQL and partly managed by the
broker.

| Use | PostgresManaged | BrokerNative |
| --- | --- | --- |
| State lives in | PostgreSQL | Message broker |
| Good for | Business jobs that need status, retries, schedules, cancellation, or operator actions | Integration events, notifications, and high-volume consumers |
| Delivery record | `JobRun`, attempts, leases, and completion are stored by KubeJob | Delivery and acknowledgement are stored by the broker |
| Idempotency | KubeJob can enforce an `IdempotencyKey` | Use a business key in the handler; no KubeJob de-duplication store is written |
| Failure handling | KubeJob retry and lease recovery | Broker retry and dead-letter policy |

Both paths are at-least-once. A successful acknowledgement cannot make an
email, payment, or HTTP call exactly-once, so handlers that touch another
system must be safe to run again.

### PostgresManaged jobs

Use this path when the job is part of a business process that someone may need
to inspect or repair later. A worker claims a lease, runs the handler, and
records the result in PostgreSQL. If a worker disappears, another worker can
recover the expired lease.

Register a typed job:

```csharp
public sealed record SendWelcomeEmail(string Address);

public sealed class SendWelcomeEmailJob : IKubeJob<SendWelcomeEmail>
{
    public ValueTask ExecuteAsync(
        SendWelcomeEmail payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        // Send the email here and pass cancellationToken to the mail client.
        return ValueTask.CompletedTask;
    }
}

var key = new JobKey<SendWelcomeEmail>("mail.send-welcome");
builder.Services.AddKubeJobHandler<SendWelcomeEmailJob, SendWelcomeEmail>(key);
```

For a small service, the control plane and worker can run in the same process:

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

Enqueue through `IJobClient`. The idempotency key is stored with the managed
job, so a repeated request can resolve to the same logical submission:

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

### BrokerNative jobs and events

Use this path when the broker should own delivery. KubeJob publishes to the
configured transport and the consumer acknowledges only after the handler
finishes. No managed `JobRun`, lease, or completion row is created for this
path.

RabbitMQ is the included adapter; Kafka is an alternative BrokerNative adapter.
Choose one transport id per logical job queue:

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

BrokerNative event consumers additionally need PostgreSQL for their durable
Inbox. Configure the server with `server.UsePostgreSql(connectionString)` and
initialize its schema before starting the consumer. The Inbox records a
successful `(EventId, capability)` before broker acknowledgement, preventing a
redelivery after that write from invoking the same capability again.

Kafka keeps the same Core contract, but maps a job queue to a Kafka topic and
maps the fixed event capabilities to three consumer groups. Replicas in the
same group distribute partitions horizontally:

```csharp
using KubeJob.Transport.Kafka;

builder.Services.ConfigureKubeJobQueueRuntimes(options =>
{
    options.Queues["order.created"] = new QueueRuntimeRoute
    {
        Mode = QueueRuntimeMode.BrokerNative,
        TransportId = KafkaBrokerNativePublisher.Id
    };
});
builder.Services.AddKafkaKubeJobBrokerNativeTransport(options =>
    options.BootstrapServers = "kafka-1:9092,kafka-2:9092");
builder.Services.AddKafkaKubeJobBrokerNativeConsumer(options =>
    options.BootstrapServers = "kafka-1:9092,kafka-2:9092");
```

For events Kafka uses one shared `order.events` topic and exactly three fixed
consumer groups: `kubejob.<environment>.log`, `.data`, and `.notify`. Register
event handlers with one of those subscription names, then call
`AddKafkaKubeJobEventConsumer`. Retry and dead-letter records stay scoped to
the capability group, so a data retry is not replayed to log or notify. Kafka
does not provide RabbitMQ-style per-message TTL; retries use visible 5s, 30s,
5m, or 30m tiers (larger requested delays are capped at 30m).

For RabbitMQ fan-out, give each event consumer a different subscription name.
Kafka deliberately restricts subscriptions to the fixed `log`, `data`, and
`notify` capabilities above. See [Event subscriptions](docs/v3/events.md) for
the topic, queue, retry, and dead-letter rules.

## Configuration and safety

- HTTP and Dashboard endpoints require authorization by default. Add an
  explicit policy and call `UseAuthentication()` and `UseAuthorization()` in
  the host pipeline.
- `AllowAnonymousEndpoints` and `AllowAnonymousAccess` are intended for local
  development and tests. Do not enable them on a public or shared network.
- Initialize the PostgreSQL schema before starting managed workers. The sample
  does this with `InitializeKubeJobDatabase()`.
- Event consumers use the same PostgreSQL schema for their durable Inbox; they
  fail at startup if no durable Inbox is configured.
- RabbitMQ BrokerNative uses one fixed retry delay per retry queue. Configure
  `RetryDelay` for the adapter; KubeJob does not create per-delay or per-worker
  retry queues.
- Kafka topic creation is disabled by default. Provision `order.events`, each
  job topic, and their `.retry`/`.dlq` companions before startup; use
  `CreateTopicsOnStartup` only for local development.
- Do not put large payloads or secrets in a job message. Store sensitive data
  in your application database and pass a reference to the handler.

## Samples

`samples/KubeJob.Sample.Unified` is the maintained end-to-end sample. It uses
PostgresManaged jobs and exposes the dashboard and demo endpoints. The
RabbitMQ integration tests under `tests/KubeJob.RabbitMqIntegrationTests`
exercise the BrokerNative job and event paths.

## Development and verification

Start dependencies, then run the release test suite:

```bash
bash scripts/dev-stack.sh up
dotnet test KubeJob.sln -c Release
```

RabbitMQ integration tests run when
`KUBEJOB_RABBITMQ_TEST_CONNECTION` contains an AMQP connection string. Without
it, those tests are skipped intentionally.

Kafka integration tests run when `KUBEJOB_KAFKA_TEST_BOOTSTRAP` contains
bootstrap servers (for the local stack: `localhost:9092`). They are skipped
when Kafka is not configured.

## Documentation

- [Runtime model](docs/v3/runtime-model.md)
- [Transport and capabilities](docs/v3/transport.md)
- [Event subscriptions](docs/v3/events.md)
- [Local development](docs/v3/local-development.md)
- [Benchmarking](docs/v3/benchmarking.md)
- [Release checklist](docs/v3/release-checklist.md)
- [中文入口](README_zh.md)

## License

[MIT License](LICENSE)
