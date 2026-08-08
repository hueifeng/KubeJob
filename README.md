# KubeJob

[中文](./README_zh.md) · [Getting Started](./docs/v2/getting-started.md) · [Architecture](./docs/v2/architecture.md) · [Local Development](./docs/v2/local-development.md)

KubeJob is a typed, embeddable distributed Job and Event runtime for .NET.

V3 is built around one rule:

> **One logical Queue has one execution authority.**

KubeJob therefore exposes two runtime modes instead of making RabbitMQ and PostgreSQL participate in the same execution transaction.

| Runtime | Execution authority | Hot path | Best fit |
|---|---|---|---|
| `BrokerNative` | message transport | publish → broker → worker → handler → ACK | high-throughput background work and events |
| `PostgresManaged` | PostgreSQL | submit Run → claim → lease → handler → complete | strongly managed jobs, fencing, strong cancellation/status |

RabbitMQ is the first implemented BrokerNative transport. The core transport seam is intentionally provider-neutral so Kafka, SQS, Redis Streams, Pulsar, or other adapters can be added later; those adapters are **not implemented yet**.

KubeJob provides **at-least-once execution**. Handlers that perform external side effects should be idempotent.

## Why V3

The previous `BrokerDispatch` design published an execution envelope to RabbitMQ and then asked PostgreSQL to admit/claim the same execution before a worker could run it. That made both systems part of the hot path.

V3 removes that dual authority:

```text
BrokerNative

IJobClient
   ↓
IMessageTransportPublisher
   ↓
RabbitMQ
   ↓
WorkerExecutionEngine
   ↓
Handler
   ↓
ACK / Retry / DLQ

Normal execution path: 0 PostgreSQL calls
```

```text
PostgresManaged

IJobClient
   ↓
PostgreSQL Run
   ↓
Claim / Attempt / Lease
   ↓
WorkerExecutionEngine
   ↓
Handler
   ↓
Durable completion
```

An optional RabbitMQ wake notification can reduce PostgresManaged polling latency, but PostgreSQL remains the authority and polling remains the correctness fallback.

## Job and Event semantics

KubeJob deliberately separates Job Queue semantics from Event Pub/Sub semantics.

### Job Queue

A Job should be completed by one worker from a worker pool:

```text
order.created queue
        │
  ┌─────┼─────┐
  ▼     ▼     ▼
 W1    W2    W3
```

Multiple worker replicas compete on the same logical Queue. A worker replica does not get its own Queue.

With RabbitMQ BrokerNative, one logical Job Queue maps to one execution queue by default. Retry and DLQ topology are transport implementation details.

### Event subscription

An Event may have multiple independent subscribers:

```text
                         order.events
                         topic/exchange
                              │
                         order.created
                              │
           ┌──────────────────┼──────────────────┐
           ▼                  ▼                  ▼
   order-business        order-log          data-clean
      subscription       subscription       subscription
           │                  │                  │
       one queue          one queue          one queue
           │                  │                  │
      workers × N        workers × N        workers × N
```

Each Subscription owns its own queue, retry path, and DLQ. If `order-log` fails, only the `order-log` delivery is retried; KubeJob never republishes the failed delivery to the Topic and accidentally replays already-successful subscribers.

## BrokerNative quick start with RabbitMQ

Register the server-side client/runtime routing, a self-contained BrokerNative worker, and the RabbitMQ adapter:

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

builder.Services.AddKubeJobHandler<OrderCreatedJob, OrderCreatedPayload>();

builder.Services.AddKubeJobBrokerNativeWorker(options =>
{
    options.WorkerId = "order-worker";
    options.Queues = new List<string> { "order.created" };
    options.MaxConcurrentJobs = 64;
});

builder.Services.AddRabbitMqKubeJobBrokerNativeConsumer(options =>
{
    options.ConnectionString = "amqp://guest:guest@localhost:5672/";
    options.PrefetchCount = 128;
});
```

Submit through the transport-neutral `IJobClient`:

```csharp
await jobs.EnqueueAsync(
    OrderCreatedJob.JobKey,
    new OrderCreatedPayload(orderId),
    new JobEnqueueOptions { Queue = "order.created" },
    cancellationToken);
```

For a BrokerNative queue this publishes a self-contained message directly to the configured transport. No `JobRun`, admission request, lease, or completion write is created in PostgreSQL.

## PostgresManaged quick start

Use the normal unified host for strong managed semantics:

```csharp
builder.Services.AddKubeJob(
    configureServer: server => server.UsePostgreSql(connectionString),
    configureWorker: worker =>
    {
        worker.WorkerId = "settlement-worker";
        worker.Queues = new List<string> { "settlement" };
        worker.MaxConcurrentJobs = 32;
    });
```

Managed queue policy controls PostgreSQL-side ordering and worker eligibility; it does not select a broker:

```csharp
builder.Services.ConfigureKubeJobQueueRouting(options =>
{
    options.Queues["settlement"] = new KubeJob.ControlPlane.Runtime.QueueDefinition
    {
        OrderingMode = ExecutionOrderingMode.KeyOrdered,
        ConsumerGroup = "settlement-workers"
    };
});
```

`PostgresManaged` retains:

- durable Run / Attempt state
- worker sessions and epochs
- leases and renewal
- fencing of stale workers
- retries and retry budget
- strong cancellation through durable database state
- `KeyOrdered` and `StrictFifo` database ordering
- durable schedule state

## Event runtime

Event publication is exposed separately through `IEventBus`; subscriptions are registered with the Event worker extension APIs. The model is intentionally not folded into `IJobClient` because the delivery semantics are different:

- Job: one successful worker execution per Queue delivery.
- Event: one delivery per Subscription, with competing workers inside each Subscription.

RabbitMQ implements Topic/Subscription queues today. Event contracts and runtime routing are transport-neutral.

## Scheduling

Schedule definitions remain durable control-plane resources in PostgreSQL.

At fire time:

```text
Schedule
   ↓
Scheduler
   ├─ PostgresManaged → create durable Run
   └─ BrokerNative    → publish self-contained message
```

For BrokerNative, the scheduler waits for the transport publish confirmation before advancing the occurrence cursor. `SkipIfRunning`, which depends on strong Run state, is intentionally a managed-runtime feature.

## Retry and failure semantics

BrokerNative uses transport-native delivery recovery:

```text
handler failure
    ↓
retry policy
    ↓
delayed retry
    ↓
execution queue
    ↓
DLQ after attempts are exhausted
```

RabbitMQ implements delayed retry/DLQ topology internally. A retry is published/confirmed before the original delivery is ACKed.

PostgresManaged uses durable Run/Attempt retry state in PostgreSQL.

## Transport architecture

Runtime code depends on KubeJob transport contracts, not RabbitMQ APIs:

```text
KubeJob Client / Runtime
          │
          ▼
IMessageTransportPublisher
          │
     ┌────┴────┐
     ▼         ▼
 RabbitMQ   future adapters
 implemented Kafka/SQS/etc.
             not yet implemented
```

RabbitMQ exchange, binding, retry queue, DLX, and DLQ names are adapter internals and are intentionally hidden from the logical Queue/Topic model and Dashboard.

## Transactional application outbox

KubeJob's internal PostgresManaged outbox is a **wake-signal outbox**, not a general business-transaction outbox package.

If an application must atomically commit business data and publish a BrokerNative Job/Event, use an application transactional-outbox pattern in the business database. A dedicated EF Core/Dapper KubeJob business-outbox integration is not part of V3 yet.

## Benchmark

`tests/KubeJob.Benchmark` compares the actual V3 authorities:

```bash
dotnet run --project tests/KubeJob.Benchmark -- \
  --runtime BrokerNative --jobs 50000

dotnet run --project tests/KubeJob.Benchmark -- \
  --runtime PostgresManaged --jobs 50000
```

The benchmark uses process-local handler completion tracking so BrokerNative does not query `JobRun` merely to measure completion. Normal PostgreSQL durability is enabled; the benchmark no longer sets `synchronous_commit=off`.

Useful metrics include ingest TPS, end-to-end TPS, P50/P95/P99 latency, RabbitMQ Ready/Unacked, CPU/memory, and PostgreSQL connection peak. A BrokerNative normal-path benchmark should observe PostgreSQL connection usage of zero from KubeJob execution.

## Reliability boundaries

KubeJob is at-least-once, not exactly-once. Design external side effects for replay.

For BrokerNative:

- broker ACK/redelivery owns crash recovery
- worker-local concurrency and broker prefetch own capacity
- transport outage affects BrokerNative delivery
- control-plane/PostgreSQL outage must not stop an already-running BrokerNative worker data plane

For PostgresManaged:

- PostgreSQL owns Run/Attempt/Lease correctness
- lease expiry is the final recovery path after worker failure
- stale session epochs and lease tokens fence late completions

## Project layout

The current repository keeps transport contracts in Core while the public abstraction stabilizes:

```text
src/
  KubeJob.Core
  KubeJob.Client
  KubeJob.ControlPlane
  KubeJob.Worker
  KubeJob.Server
  KubeJob.Storage.PostgreSQL
  KubeJob.Transport.RabbitMQ
```

The important dependency rule is semantic rather than naming-based: Core/Client/Worker execution logic must not depend on `RabbitMQ.Client`; only the RabbitMQ adapter may contain broker-specific topology and protocol code.

## Status

V3 currently implements:

- `PostgresManaged`
- RabbitMQ `BrokerNative` Job consumption
- transport-neutral BrokerNative publication
- Event Topic + independent Subscription queues on RabbitMQ
- subscription-scoped retry/DLQ
- shared `WorkerExecutionEngine`
- schedule routing by Queue runtime authority
- Dashboard runtime-authority view
- V3 PostgresManaged/BrokerNative benchmark harness

Not implemented yet:

- Kafka/SQS/Redis/Pulsar transport adapters
- BrokerNative transport-native partition ordering API
- dedicated application transactional-outbox package
- workflow engine

See the ADRs under [`docs/adr`](./docs/adr) for the architectural decisions.