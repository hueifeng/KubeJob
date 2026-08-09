# KubeJob V3 Transport Model

Transport adapters isolate external messaging systems from runtime logic.

## Capability model

Runtime code should depend on declared transport capabilities instead of leaking broker-specific behavior.

A transport capability model includes:

```text
TransportCapabilities

- Durable
- OrderedDelivery
- DelayDelivery
- DeadLetter
- Cancellation
- ExactlyOnce
```

Capabilities describe what a transport can provide; they do not change the execution authority model.

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

The runtime does not assume RabbitMQ is the only future transport.

## Future transports

Kafka, Pulsar and other brokers can be integrated through the same adapter boundary.

New transports should expose their capabilities and keep broker-specific details inside the adapter layer.
