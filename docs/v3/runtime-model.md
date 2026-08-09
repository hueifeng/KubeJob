# KubeJob V3 Runtime Model

KubeJob V3 separates two execution models. They share handler abstractions but have different execution authorities.

## Managed Runtime

Managed Runtime is designed for business tasks that require lifecycle management.

Authority:

```
PostgreSQL
```

Lifecycle:

```
Submit
 |
JobRun
 |
Attempt
 |
Lease
 |
Execute
 |
Complete
```

Characteristics:

- persistent execution state
- retry and cancellation management
- worker fencing
- operational query capability
- recovery after worker failure

Typical scenarios:

- order processing
- settlement tasks
- scheduled business jobs
- long-running workflows

## BrokerNative Runtime

BrokerNative is designed for event-driven workloads with high throughput requirements.

Authority:

```
Message Broker
```

Lifecycle:

```
Publish
 |
Exchange
 |
Queue
 |
Consumer
 |
Handler
 |
ACK
```

Characteristics:

- high throughput delivery
- independent subscribers
- transport controlled retry/dead-letter behavior
- no managed Run/Attempt lease path

Typical scenarios:

- domain events
- logging pipelines
- data synchronization
- notifications

## Design Rule

Managed Runtime answers:

> What is the state of this business task?

BrokerNative answers:

> How do we deliver this event efficiently?

The two models are complementary and should coexist in one platform.
