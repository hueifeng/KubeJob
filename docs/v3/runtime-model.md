# Runtime model

KubeJob has two execution paths. They share typed handlers, but they do not
share ownership of delivery state. Configure a queue as either
`PostgresManaged` or `BrokerNative` and keep that choice visible in your
service configuration.

## PostgresManaged

PostgresManaged is for work that has a business lifecycle: processing an
order, generating a report, or running a task that an operator may need to
retry or cancel.

The flow is:

```text
client → PostgreSQL JobRun → worker lease → handler → completion in PostgreSQL
```

PostgreSQL records the job, each attempt, the lease owner, retry timing, and
the final result. A worker that stops heartbeating loses its lease; another
worker can recover the job after the lease timeout. RabbitMQ may be configured
as a wake-up notification, but it never grants execution ownership on this
path.

Use this path when you need one or more of:

- an idempotency key enforced by KubeJob;
- a durable status page or API for operators;
- scheduled or delayed execution;
- cancellation and timeout state recorded with the job;
- retry and worker-fencing decisions made by KubeJob.

### Fencing, completion, and timeouts

Every managed claim carries a monotonically increasing `FenceVersion` and a
lease token. Renewals and completion acceptance must present both values. If a
worker loses its lease before the completion intent is accepted, that stale
completion is rejected.

A valid completion first crosses a durable boundary in PostgreSQL:

```text
Running attempt
    ↓ persist immutable completion intent
Completing attempt
    ↓ finalize Run/Attempt state
Succeeded / Pending / Failed / Dead / Canceled
```

Once the completion intent is persisted, the attempt no longer depends on the
original worker lease or session. Lease recovery and timeout scanning only
reclaim `Running` attempts, so a control-plane restart cannot turn an already
accepted completion into a retry. The recovery dispatcher replays persisted
intents after restart. A conflicting duplicate completion for the same attempt
is rejected rather than overwriting the original outcome.

`TimeoutSeconds` is authoritative. The worker links the handler cancellation
token to the timeout and reports `TimedOut` even if a handler ignores
cancellation and returns late. The control plane also compares the durable
completion-acceptance time with the attempt deadline; a completion accepted
after that deadline is finalized as `TimedOut`. This makes success-vs-timeout
independent of whether the worker completion call or timeout scanner happens
to acquire a database lock first.

Retry behavior is selected from the per-run `RetryPolicy` when present, then
falls back to the server policy. The policy controls fixed, linear,
exponential, and jittered backoff; `MaxAttempts` remains the retry budget.

Handlers receive the current attempt context:

```csharp
public ValueTask ExecuteAsync(
    OrderPayload payload,
    JobExecutionContext context,
    CancellationToken cancellationToken)
{
    // Use context.FenceVersion when an external store supports fencing.
    // Always pass cancellationToken to downstream calls.
    return ValueTask.CompletedTask;
}
```

Persisted completion state does not make external side effects exactly-once.
Business handlers must still make duplicate-sensitive external writes
idempotent.

### Ordering

`KeyOrdered` serializes work sharing the same `ConcurrencyKey`.
`BestEffortFifo` serializes visible work for one managed execution lane in
submission-sequence order. It is intentionally not advertised as strict global
FIFO: two concurrent submission transactions can allocate sequence values and
commit in the opposite order under PostgreSQL `READ COMMITTED` visibility.
`StrictFifo` remains a compatibility alias for the same numeric mode; new
configuration should use `BestEffortFifo`.

## BrokerNative

BrokerNative is for transport-first work: integration messages, notifications,
or consumers where the broker already provides the delivery features you need.

The flow is:

```text
publisher → broker exchange/topic → queue/subscription → handler → ACK
```

The broker owns delivery, redelivery, and dead-letter routing. KubeJob does
not create a managed `JobRun`, lease, or completion record for a BrokerNative
message. A different subscription name creates a different delivery stream;
replicas using the same name compete for that stream.

Choose this path when:

- broker throughput and consumer isolation matter more than a KubeJob status
  record;
- the broker's retry and dead-letter features are the desired operational
  controls;
- the handler can safely process a duplicate message using a business key.

Managed-only options such as `IdempotencyKey`, `NotBefore`, and `Priority` are
rejected on BrokerNative queues. If the application needs those semantics, use
a PostgresManaged queue instead.

## What the split means in practice

The same process may host both paths, but a single logical queue has one owner.
Do not publish a BrokerNative message and then try to claim it through the
managed lease tables. Likewise, do not treat a PostgreSQL wake-up notification
as the queue itself.

For the configuration examples, see [Transport and capabilities](transport.md)
and [Event subscriptions](events.md). For a working local setup, start with
[Local development](local-development.md).
