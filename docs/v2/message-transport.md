# Message Transport Adapters

KubeJob keeps durable state, claims, leases, retries, cancellation, and
Schedules in the control plane. A message transport only communicates the
non-authoritative fact that a Queue may have claimable work.

This wake-up role is separate from business-message ingress. An ingress Adapter
subscribes to RabbitMQ, Kafka, or another broker and submits a durable Run
through `JobControlPlane`. An application-specific typed consumer may instead
use `IJobClient`. In either case it acknowledges the source message only after
acceptance. See [ADR 008](../adr/008-separate-message-ingress-from-worker-wakeup.md).

`ControlPlaneValidationException` identifies a permanent malformed message.
Storage, connectivity, and cancellation failures remain transient failures for
the ingress adapter to retry according to broker policy.

The included RabbitMQ business-message Adapter can be registered with:

```csharp
builder.Services.AddKubeJobServer(options =>
    options.UsePostgreSql(connectionString));

builder.Services.AddRabbitMqKubeJobIngress(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
    options.ExchangeName = "kubejob.job-ingress";
    options.QueueName = "mailer-ingress";
    options.RoutingKey = "mail.#";
    options.Source = "rabbitmq.mailer";
});
```

The queue is durable and uses manual acknowledgements. A valid message is ACKed
after the Run plus Outbox transaction has committed. Invalid JSON, invalid job
fields, and idempotency conflicts are rejected without requeue so a configured
dead-letter exchange can retain them. Store, database, and network failures are
requeued.

## Adapter seam

The control plane publishes a strongly typed `WorkAvailableSignal` through
`IWorkAvailableNotifier`. The signal contains only a schema version, Outbox
event ID, Queue, and Run ID. It never carries a Job payload, lease token, or
authority to execute.

Transport packages select their publisher with:

```csharp
services.UseKubeJobWorkAvailableNotifier<MyTransportPublisher>();
```

For high-throughput execution, the platform has a separate
`IExecutionDispatcher` seam. Its `ExecutionEnvelope` carries the accepted
logical `RunId` and Queue, but not a lease or execution authority. The included
RabbitMQ publisher can be registered with:

```csharp
services.UseRabbitMqKubeJobExecutionDispatcher(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
    options.ExchangeName = "kubejob.execution";
});
```

This publisher is only the durable, confirmed hand-off. The RabbitMQ Worker
Consumer performs targeted Admission for the envelope's RunId, reuses the
normal WorkerRuntimeService Handler/Lease/Complete path, and acknowledges only
after durable completion or an explicit terminal/rejection decision. Temporary
capacity, fencing, or database failures are requeued.

`AddKubeJobWorker` registers a broker-neutral `IWorkerClaimTrigger`. A remote
transport listener injects `IWorkerClaimTriggerSource` and calls `Pulse()`.
Repeated notifications coalesce, and the worker still performs its normal
authoritative HTTP Claim. Without a pulse, the same trigger completes after the
configured polling interval, so the worker waits only once per empty claim.

## Required adapter behavior

- Route signals by KubeJob Queue where the broker supports routing.
- Treat duplicate, delayed, missing, and out-of-order signals as normal.
- Keep periodic polling enabled; a signal is an accelerator, not a liveness
  dependency.
- Acknowledge broker delivery after the local wake signal is accepted, never
  after the Job handler completes.
- Do not put Job payloads, credentials, lease tokens, or fencing values in a
  signal.

RabbitMQ is the included adapter. Other transports, such as Kafka, NATS, Azure
Service Bus, Redis Streams, or an internal bus, can be shipped as independent
packages without changing the KubeJob state machine.
