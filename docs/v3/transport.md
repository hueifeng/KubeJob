# Transport adapters

KubeJob keeps broker client libraries out of the core runtime. A transport
adapter publishes messages, consumes them, and reports the features it can
actually provide. The runtime checks those features when a queue is
configured; it does not silently emulate a missing broker feature with a
PostgreSQL write.

## Capabilities

Adapters advertise a `MessageTransportCapabilities` value. The current flags
are:

```text
DurablePublish       the broker confirms a publish that survives a restart
OrderedDelivery     messages can be delivered in a defined order
DelayedDelivery     the broker can hold a message until a later time
DeadLetter           failed messages can be moved to a dead-letter route
ConsumerGroups       replicas can share one consumer group
Partitioning        messages can be assigned to stable partitions
Replay               a consumer can read a retained range again
```

These are capabilities, not promises that every queue uses every feature.
There is no `ExactlyOnce` flag. Durable publish plus acknowledgement still
allows a handler to run twice after a network or process failure.

## RabbitMQ

RabbitMQ is the included adapter. For a BrokerNative job queue, register the
publisher, map the logical queue to RabbitMQ, and start the native consumer:

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
    options.ConnectionString = connectionString);
builder.Services.AddRabbitMqKubeJobBrokerNativeConsumer(options =>
    options.ConnectionString = connectionString);
```

RabbitMQ owns the acknowledgement and redelivery loop on this path. A retry
policy that asks for more than one attempt requires the adapter's
`DeadLetter` capability. A queue that requires durable publish or dead-letter
handling fails fast if the selected adapter does not advertise it.

For PostgresManaged workers, RabbitMQ can be registered as an optional wake-up
notification. Losing RabbitMQ only makes the worker poll PostgreSQL; it does
not change the queue authority.

## Kafka

Kafka is a BrokerNative adapter. It uses the same Core publisher seam as
RabbitMQ, but maps a logical job queue to a durable Kafka topic and scales
through consumer-group partition assignment:

```csharp
builder.Services.AddKubeJobServer();
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

With the default options, logical queue `order.created` maps to these topics:

```text
kubejob.jobs.order.created          main delivery topic
kubejob.jobs.order.created.retry    retry topic
kubejob.jobs.order.created.dlq      terminal failure topic
```

All replicas using the same `Environment` share the
`kubejob.<environment>.jobs` consumer group. Kafka preserves order within one
partition only; set `EventPublishOptions.PartitionKey` when event ordering per
business key matters. Do not rely on cross-message ordering for BrokerNative
jobs.

The producer uses `acks=all` and idempotent producer settings. Consumers turn
off auto-commit and commit an offset only after the handler succeeds, a retry
record has been durably published, or a dead-letter record has been durably
published. Kafka event consumers additionally use the PostgreSQL Event Inbox
by `(EventId, capability)` before acknowledging a successful handler. Configure
`AddKubeJobServer(options => options.UsePostgreSql(connectionString))` for any
Event consumer; an unconfigured durable Inbox fails the consumer at startup.

Kafka topic creation is disabled by default. Provision the main, `.retry`, and
`.dlq` topics in production, with partitions and replication appropriate to
the deployment. `CreateTopicsOnStartup` is intended only for local development
and integration tests. The local Podman stack exposes Kafka on `localhost:9092`.

Kafka does not provide RabbitMQ-style per-message TTL. BrokerNative retries are
rounded to explicit 5s, 30s, 5m, or 30m tiers; requests beyond 30m are capped
at the final tier. This makes retry behavior visible instead of silently
claiming arbitrary delayed delivery.

## BrokerNative submission rules

`JobEnqueueOptions.IdempotencyKey` is a PostgresManaged feature. BrokerNative
does not maintain a KubeJob de-duplication table, so the option is rejected
instead of being accepted with weaker semantics. Put a business identifier in
the payload and make the handler check it if de-duplication is required.

The same rule applies to managed scheduling and priority: configure a managed
queue when KubeJob must own those state transitions.

## Adding another broker

An adapter should keep connection, serialization, topology, acknowledgement,
retry, and dead-letter details inside its transport project. It should expose a
stable transport id and an honest capability set. The core runtime should be
usable in tests without loading a broker client package.
