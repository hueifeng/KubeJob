# KubeJob V3 Runtime Model

KubeJob V3 separates two execution models.

## Managed Runtime

Managed Runtime is designed for business jobs that require lifecycle management.

Characteristics:

- PostgreSQL is the execution authority.
- Runs, Attempts and leases are persisted.
- Retry, cancellation and audit history are managed by KubeJob.
- Workers claim work only when capacity is available.

Typical scenarios:

- Order processing
- Settlement tasks
- Scheduled business jobs
- Long-running workflows

## BrokerNative Runtime

BrokerNative is designed for high-throughput events.

Characteristics:

- Message broker owns delivery semantics.
- Worker consumes messages directly.
- ACK, retry and dead-letter behavior belong to the transport adapter.
- No managed Run/Attempt lease path is involved.

Typical scenarios:

- Domain events
- Logging pipelines
- Data synchronization
- Notifications

## Design rule

Managed Runtime answers: "what is the state of this business task?"

BrokerNative answers: "how do we deliver this event efficiently?"

The two models share handler abstractions but do not share execution authority.
