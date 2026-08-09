# KubeJob V3 Transport Model

Transport adapters isolate external messaging systems from runtime logic.

## Capability model

Runtime code should depend on declared transport capabilities instead of leaking broker-specific behavior.

A transport capability model includes the following declared flags:

```text
MessageTransportCapabilities

- DurablePublish
- OrderedDelivery
- DelayedDelivery
- DeadLetter
- ConsumerGroups
- Partitioning
- Replay
```

Capabilities describe what a transport can provide; they do not change the
execution authority model. KubeJob does not advertise an `ExactlyOnce`
capability: durable publish and consumer acknowledgement still provide
at-least-once delivery, so handlers must tolerate duplicate delivery.

## Runtime boundary

```text
Runtime
   |
   | depends on capabilities
   |
Transport Abstraction
   |
+----------+----------+
|                     |
RabbitMQ            Kafka/Pulsar/Other
Adapter             Adapter
```

The core runtime must never reference a specific broker client library.

## RabbitMQ

RabbitMQ is one supported transport implementation.

BrokerNative RabbitMQ owns:

- message delivery
- acknowledgement
- retry strategy
- dead-letter routing

### BrokerNative submission constraints

`JobEnqueueOptions.IdempotencyKey` is a PostgresManaged feature. BrokerNative
does not maintain a KubeJob-side durable de-duplication store, so it rejects
that option instead of creating a misleading idempotency guarantee. Choose a
PostgresManaged queue when KubeJob must own idempotency, or make the handler
idempotent using a business-level key.

BrokerNative queues also require an adapter that advertises `DurablePublish`.
Queues that request retries (`MaxAttempts > 1`) additionally require
`DeadLetter`; unsupported capabilities are rejected at submission time.

The runtime does not assume RabbitMQ is the only future transport.

## Future transports

Kafka, Pulsar and other brokers can be integrated through the same adapter boundary.

New transports should expose their capabilities and keep broker-specific details inside the adapter layer.
