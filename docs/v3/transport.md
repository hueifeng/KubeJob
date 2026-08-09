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

## BrokerNative submission rules

`JobEnqueueOptions.IdempotencyKey` is a PostgresManaged feature. BrokerNative
does not maintain a KubeJob de-duplication table, so the option is rejected
instead of being accepted with weaker semantics. Put a business identifier in
the payload and make the handler check it if de-duplication is required.

The same rule applies to managed scheduling, priority, continuations, and
compensations: configure a managed queue when KubeJob must own those state
transitions.

## Adding another broker

An adapter should keep connection, serialization, topology, acknowledgement,
retry, and dead-letter details inside its transport project. It should expose a
stable transport id and an honest capability set. The core runtime should be
usable in tests without loading a broker client package.
