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
`IWorkAvailableNotifier`. The signal carries a schema version, Outbox event ID,
Queue, Run ID, and the run's `ExecutionLane`, `ConsumerGroup`, and
`PartitionKey` (so transport adapters can co-locate same-key runs on one lane).
It never carries a Job payload, lease token, or authority to execute.

Transport packages select their publisher with:

```csharp
services.UseKubeJobWorkAvailableNotifier<MyTransportPublisher>();
```

For high-throughput execution, the platform has a separate
`IExecutionTransport` seam. Its `ExecutionEnvelope` carries the accepted
logical `RunId`, Queue, and EventId, but not a lease or execution authority.
`QueueDeliveryOptions.Defaults.Profile` defaults to
`ExecutionDeliveryProfile.BrokerDispatch` (see [ADR 014](../adr/014-promote-brokerdispatch-to-default-delivery-profile.md)),
so a host that wires the RabbitMQ execution extensions below gets targeted
broker admission for every queue unless it pins a specific queue back to
`Pull` via a per-queue `QueueDefinition`. A host that does not want a broker dependency must
either register the extensions or explicitly set `Defaults.Profile = Pull`;
otherwise `UnconfiguredExecutionTransport` throws at dispatch time. The
included RabbitMQ publisher can be registered with:

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
with the logical queue as routing key; `RabbitMqTopologyProvisioner` declares one
physical execution queue per logical queue by default, for example
`kubejob.execution.{group}.mail.send.queue`. The
RabbitMQ Worker Consumer performs targeted Admission for the envelope's RunId,
reuses the normal WorkerRuntimeService Handler/Lease/Complete path, and
acknowledges only after durable completion or an explicit terminal/rejection
decision. Temporary capacity, fencing, or database failures are requeued.

### Queue policy

Each logical queue carries one `QueueDefinition` (`QueueDeliveryOptions.Queues`):
profile, ordering mode, lane, consumer group, and transport. Queues without an
entry use `QueueDeliveryOptions.Defaults`. Business callers still submit only a
logical queue name — none of these choices are visible to `IJobClient`.

```csharp
services.ConfigureKubeJobQueueRouting(routing =>
{
    // Global default: everything not listed below.
    routing.Defaults.Profile = ExecutionDeliveryProfile.BrokerDispatch;

    // One definition per queue. A queue with no entry uses Defaults.
    routing.Queues["orders"] = new QueueDefinition
    {
        Profile = ExecutionDeliveryProfile.BrokerDispatch,
        OrderingMode = ExecutionOrderingMode.KeyOrdered,
        ConsumerGroup = "orders-push",
        ExecutionLane = "default",
        TransportId = "rabbitmq"
    };
    routing.Queues["audit"] = new QueueDefinition
    {
        Profile = ExecutionDeliveryProfile.Pull   // keep this queue on polling
    };
});
```

A worker serves a queue by declaring it in `KubeJobWorkerOptions.Queues`; its
`ConsumerGroup` (and `ExecutionLane`) must match the queue's definition — the
topology provisioner fails startup on any mismatch instead of silently
never receiving work.

### Direct Dispatch topology and headers

The RabbitMQ adapter declares the following topology once at startup via
`RabbitMqTopologyProvisioner` (an `IHostedService` that retries a bounded
number of times and then fails startup with a clear error). The worker
consumer does **not** actively declare anything: it passively verifies that
its dispatch queues and the group/retry/cancel exchanges exist and fails fast
if a host joins with options that do not match the provisioned topology,
instead of looping forever on `406 PRECONDITION_FAILED`.

| Resource | Name | Type | Notes |
| --- | --- | --- | --- |
| Group exchange | `kubejob.execution.{group}` | direct, durable | Each logical execution queue binds its own routing key. Names derive from `RabbitMqExecutionOptions.ConsumerQueuePrefix` (default `kubejob.execution`) + `ConsumerGroup`. |
| Group DLX | `kubejob.execution.{group}.dlx` | fanout, durable | Catches poison envelopes whose `x-delivery-count` has saturated. |
| Group DLQ | `kubejob.execution.{group}.dlq.queue` | quorum, durable | Bound to the group DLX; inspect here for permanently failed envelopes. |
| Dispatch queue | `kubejob.execution.{group}.{logical-queue}.queue` | quorum, durable | One stable queue per business logical queue (and per lane when configured); the logical name is literal, with no hash suffix. `x-dead-letter-exchange` is set to the group DLX. `x-delivery-limit` is disabled by default and must only be enabled with a Pending-Run DLQ re-drive policy. |
| Retry exchange | `kubejob.execution.{group}.retry` | direct, durable | Temporary admission/capacity failures are republished here instead of incrementing the dispatch queue delivery count. |
| Retry queue | `kubejob.execution.{group}.retry.queue` | quorum, durable | One group-scoped technical retry queue. It binds every business routing key, uses `x-message-ttl = RetryDelay`, and dead-letters back to the group exchange with the original routing key. Normal business backlog never accumulates here. |
| Cancel exchange | `kubejob.execution.{group}.cancel` | fanout, durable | Cancel markers fan out to every worker queue in the group. |
| Per-worker cancel queue | `kubejob.execution.{group}.cancel.{worker-id}` | auto-delete | One ephemeral queue per **stable WorkerId** (not per SessionId), so a restart reuses the same queue name instead of churning new ephemeral queues in the management UI. The queue is removed automatically when the worker disconnects, so retired workers do not accumulate queues. The durable cancel Outbox row and lease fallback provide correctness across restarts and overlapping drains. |

### Queue lifecycle and operations

- **Who creates queues.** `RabbitMqTopologyProvisioner` declares the group
  topology (exchanges, retry queue, DLX/DLQ, one dispatch queue per
  logical queue and lane) for the queues registered by the worker at startup.
  The publisher (dispatcher) and the consumer never declare topology; the
  consumer passively verifies that its queues exist and fails fast on a
  mismatch. Restarting a host with unchanged options is a no-op on the broker:
  the same physical queue names are reused, nothing accumulates.
- **Stable names, ephemeral cancel queues.** Durable dispatch/retry/DLQ names
  are stable across restarts and must never be suffixed with process IDs or
  session IDs. The only ephemeral queue is the per-worker cancel queue, whose
  name is derived from the stable WorkerId and which auto-deletes when the
  worker disconnects.
- **Admission batching.** The worker consumer collects up to
  `RabbitMqExecutionOptions.AdmissionBatchSize` (default 16) deliveries and
  admits them in one control-plane claim transaction, so per-envelope
  admission round trips amortize to roughly two database transactions per
  batch (one claim, one diagnostic read for unclaimed envelopes). Per-envelope
  ACK/reject/retry semantics are unchanged. Set `PrefetchCount` at least as
  large as `AdmissionBatchSize` so batches fill; capacity, fencing, and
  ordering gates are per-Run and identical to the unbatched path.
- **Operations surface.** The Dashboard Queue inventory page
  (`/queues`) lists every configured or worker-registered logical queue with
  its resolved profile, lane, group, ordering mode, transport, and the
  physical RabbitMQ queue names it maps to. The logical-to-physical naming
  contract lives entirely in `RabbitMqExecutionOptions`.

### Queue-name migration

The physical queue names are part of the deployment topology. When upgrading from
an older release that used names without the `.queue` suffix, stop the old
publisher/consumer, drain or re-drive messages from the old execution/retry/DLQ
queues, verify `messages_ready=0`, `messages_unacknowledged=0`, and
`consumers=0`, then remove the old bindings and queues before switching traffic
to the new names. A name change alone does not move existing RabbitMQ messages.

Changing to the per-logical-queue topology is a clean deployment topology
change: provision the business queues before switching publisher traffic. The
worker consumes `{prefix}.{group}.{logical-queue}.queue`; the group-scoped retry
queue remains `{prefix}.{group}.retry.queue`.

The stable names are intentionally reused across service restarts; do not append
process IDs, Pod UIDs, or random GUIDs to durable execution or ingress queues.
The physical execution names preserve the literal business logical queue name
and end in `.queue`; retry and DLQ remain group-scoped technical queues. The
control plane never sees RabbitMQ-side names; `RabbitMqExecutionOptions` owns
the logical-to-physical routing contract.

Per-message header conventions:

- Dispatch envelopes set `properties.Type = "execution-envelope"` and `MessageId = EventId`. Consumers dispatch on type, not on body parsing.
- Cancel markers set `properties.Type = "cancel"` and `X-KubeJob-Event-Type = "cancel"`. Each worker-session cancel consumer calls the in-flight attempt cancellation hook and ACKs the marker.
- Quorum queues track `x-delivery-count` authoritatively across consumer restarts. KubeJob Retry and transient exceptions are republished to the group TTL retry queue and the original delivery is ACKed only after retry publication is confirmed. A retry-publication failure is a `Reject` (requeue=false), routing the envelope through the group DLX rather than NACKing it back to the head of the queue (which would loop indefinitely); `Nack(requeue=true)` is reserved for cancel-marker transient failures, and shutdown leaves deliveries unacked so the broker requeues them on connection close.

Retry ownership: KubeJob's `MaxAttempts` (enforced by the completion store)
remains the authoritative terminal-state driver. The group TTL retry queue owns
only transient broker/worker admission delay; it does not create a new
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
