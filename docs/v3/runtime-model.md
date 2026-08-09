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

Managed-only options such as `IdempotencyKey`, `NotBefore`, `Priority`,
continuations, and compensations are rejected on BrokerNative queues. If the
application needs those semantics, use a PostgresManaged queue instead.

## What the split means in practice

The same process may host both paths, but a single logical queue has one owner.
Do not publish a BrokerNative message and then try to claim it through the
managed lease tables. Likewise, do not treat a PostgreSQL wake-up notification
as the queue itself.

For the configuration examples, see [Transport and capabilities](transport.md)
and [Event subscriptions](events.md). For a working local setup, start with
[Local development](local-development.md).
