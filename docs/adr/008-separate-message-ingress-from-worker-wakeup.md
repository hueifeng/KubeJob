# ADR 008: Separate message ingress from worker wake-up

- Status: Accepted
- Date: 2026-07-26

## Context

Applications commonly use RabbitMQ, Kafka, NATS, or a cloud bus in two
fundamentally different roles:

1. business messages enter KubeJob and become durable logical Runs;
2. the KubeJob control plane hints that a Queue may contain claimable work.

Combining those roles would make broker acknowledgement, payload delivery,
Worker ownership, retries, cancellation, and fencing part of one shallow
transport Interface. Broker-specific behavior would then leak into handlers and
the authoritative JobRun state machine.

## Decision

KubeJob models message ingress and Worker wake-up as separate Modules.

### Message ingress

An ingress Adapter continuously consumes its broker's business messages, maps
each message to an `EnqueueJobRequest`, and calls `JobControlPlane`. A
business-specific typed Adapter may call `IJobClient` instead.

- The Adapter acknowledges RabbitMQ delivery or commits a Kafka offset only
  after KubeJob has durably accepted the JobRun.
- The broker message identity should become the KubeJob `IdempotencyKey`.
- Broker redelivery is normal and must not create a second logical Run.
- Broker-native subscription, partition, acknowledgement, and retry behavior
  remains local to that Adapter.

### Worker wake-up

The transactional Outbox publishes `WorkAvailableSignal` through
`IWorkAvailableNotifier`.

- The signal contains no Job payload, LeaseToken, or authority to execute.
- A Worker listener only pulses `IWorkerClaimTriggerSource`.
- The Worker still claims, renews, and completes through the authoritative
  control plane.
- Periodic polling remains the liveness fallback.

RabbitMQ Worker listeners use shared competing-consumer queues within a
configured `ConsumerGroup`. Different groups are independent Worker pools.

The included RabbitMQ business-message Adapter uses a durable queue with manual
acknowledgements. It ACKs after durable submission, rejects permanent invalid
or conflicting messages for dead-letter handling, and requeues transient
failures. Its exchange and queue are configured independently from the
work-available notification exchange.

## Consequences

- RabbitMQ, Kafka, and other ingress packages can be added without changing
  handlers or JobRun ownership.
- Wake-up broker failure affects latency rather than correctness.
- Ingress acknowledgement has a clear durable hand-off point.
- Permanent `ControlPlaneValidationException` failures can be dead-lettered
  without misclassifying storage or connectivity failures.
- The same broker may be used for both roles, but the exchanges, topics,
  messages, and Interfaces remain distinct.
- Full broker-direct Job delivery remains a separate future design. It cannot
  be implemented by expanding `IWorkAvailableNotifier`.
