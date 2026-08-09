# KubeJob V3 Transport Model

Transport adapters isolate external messaging systems from runtime logic.

## Capability model

A transport should declare capabilities instead of leaking implementation details.

Example capabilities:

- Durable delivery
- Ordered delivery
- Delay support
- Dead-letter support
- Cancellation support
- Strong status tracking

Runtime code must only depend on capabilities.

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

The core model should never reference a specific broker client library.
