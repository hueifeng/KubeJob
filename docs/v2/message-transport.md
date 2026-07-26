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
    options.DeadLetterExchangeName = "kubejob.job-ingress.dlx";
    options.DeadLetterRoutingKey = "dead";
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
logical `RunId`, Queue, and EventId, but not a lease or execution authority.
The included RabbitMQ publisher can be registered with:

```csharp
services.UseRabbitMqKubeJobExecutionDispatcher(options =>
{
    options.ConnectionString = "amqp://kubejob:secret@rabbitmq:5672/";
    options.ConsumerGroup = "order-push";
});
```

This publisher is only the durable, confirmed hand-off. It publishes each
`ExecutionEnvelope` to the per-group direct exchange
`{ConsumerQueuePrefix}.{ConsumerGroup}` (default `kubejob.execution.{group}`)
with the logical queue as routing key; `RabbitMqDispatchTopology` declares that
exchange and binds each queue's quorum queue to it on worker startup. The
RabbitMQ Worker Consumer performs targeted Admission for the envelope's RunId,
reuses the normal WorkerRuntimeService Handler/Lease/Complete path, and
acknowledges only after durable completion or an explicit terminal/rejection
decision. Temporary capacity, fencing, or database failures are requeued.

### Direct Dispatch topology and headers

The RabbitMQ adapter for Direct Dispatch Mode declares the following topology
on worker startup via `RabbitMqDispatchTopology`:

| Resource | Name | Type | Notes |
| --- | --- | --- | --- |
| Group exchange | `kubejob.execution.{group}` | direct, durable | All per-queue dispatch queues bind here with their `Queue` as routing key. Names derive from `RabbitMqExecutionOptions.ConsumerQueuePrefix` (default `kubejob.execution`) + `ConsumerGroup`. |
| Group DLX | `kubejob.execution.{group}.dlx` | fanout, durable | Catches poison envelopes whose `x-delivery-count` has saturated. |
| Group DLQ | `kubejob.execution.{group}.dlq` | quorum, durable | Bound to the group DLX; inspect here for permanently failed envelopes. |
| Per-queue dispatch queue | `kubejob.execution.{group}.{sanitized-queue}` | quorum, durable | `x-dead-letter-exchange` set to the group DLX; `x-delivery-limit` is disabled by default and must only be enabled with a Pending-Run DLQ re-drive policy. |
| Retry exchange | `kubejob.execution.{group}.retry` | direct, durable | Temporary admission/capacity failures are republished here instead of incrementing the dispatch queue delivery count. |
| Per-queue retry queue | `kubejob.execution.{group}.retry.{sanitized-queue}` | quorum, durable | Uses `x-message-ttl = RetryDelay` and dead-letters back to the group exchange with the logical queue routing key. |
| Cancel exchange | `kubejob.execution.{group}.cancel` | fanout, durable | Cancel markers fan out to every worker queue in the group. |
| Per-worker cancel queue | `kubejob.execution.{group}.cancel.{worker-session}` | exclusive, auto-delete | One ephemeral queue per worker session, bound to the cancel exchange; the durable cancel Outbox row and lease fallback provide correctness across restarts. |

`{sanitized-queue}` is the lower-cased, alphanumeric-and-dash form of the
KubeJob logical queue name (continuous `-` collapsed, capped at 48 characters,
and always suffixed with a stable 6-character hash to prevent collisions).
The control plane never sees RabbitMQ-side names; `RabbitMqExecutionOptions.GetConsumerQueueName`
is the only place a logical queue becomes a physical RabbitMQ queue name.

Per-message header conventions:

- Dispatch envelopes set `properties.Type = "execution-envelope"` and `MessageId = EventId`. Consumers dispatch on type, not on body parsing.
- Cancel markers set `properties.Type = "cancel"` and `X-KubeJob-Event-Type = "cancel"`. Each worker-session cancel consumer calls the in-flight attempt cancellation hook and ACKs the marker.
- Quorum queues track `x-delivery-count` authoritatively across consumer restarts. KubeJob Retry and transient exceptions are republished to the per-queue TTL retry queue and the original delivery is ACKed only after retry publication is confirmed; direct `Nack(requeue=true)` is reserved for retry-publication failure or shutdown fallback.

Retry ownership: KubeJob's `MaxAttempts` (enforced by the completion store)
remains the authoritative terminal-state driver. The per-queue TTL retry queue
owns only transient broker/worker admission delay; it does not create a new
Attempt or increment `MaxAttempts`. `x-delivery-limit` is disabled by default;
if explicitly enabled, the deployment must provide a DLQ re-drive policy for
Pending Runs rather than treating the broker DLQ as the business state machine.

Cancel ownership: the cancel outbox row is durable and is the single source
of truth for cancel propagation. The RabbitMQ cancel exchange is a low-latency
fanout accelerator; the lease reaper and renewal loop remain the correctness
fallback if the broker is unavailable.

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
