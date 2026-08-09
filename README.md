# KubeJob

KubeJob is a **Distributed Job & Event Runtime for .NET**.

It provides two execution models with different authorities:

```
                         KubeJob
                            |
          +-----------------+-----------------+
          |                                   |
 Managed Runtime                     BrokerNative Runtime
          |                                   |
 PostgreSQL                          Message Broker
```

## Runtime Model

### Managed Runtime

Use when the business process requires lifecycle management:

- order processing
- settlement
- financial tasks
- long running workflows
- manual recovery

PostgreSQL is the source of truth:

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

Provides:

- persistent state
- retry control
- cancellation
- worker fencing
- operational query

### BrokerNative Runtime

Use for high-throughput event scenarios:

- order events
- logging
- data synchronization
- notifications
- integration events

Message broker owns delivery:

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

Provides:

- high throughput
- independent subscribers
- transport-level retry/dead-letter handling
- decoupled consumers

BrokerNative delivery is at-least-once. It deliberately rejects
`JobEnqueueOptions.IdempotencyKey`, because durable KubeJob-side de-duplication
belongs to the Managed Runtime. Use a PostgresManaged queue when KubeJob must
own that guarantee, or make the handler idempotent with a business key.

## Performance

Benchmark results (environment dependent):

| Runtime | Complete TPS | P95 | DB writes |
| --- | ---: | ---: | ---: |
| BrokerNative | 26,701 | 4.28 ms | 0 |
| Managed Runtime | 7,651.9 | 1,254 ms | 30,000 |

The two models are complementary:

- Managed optimizes for governance and state management.
- BrokerNative optimizes for throughput and event distribution.

## Quick Start

```csharp
builder.Services.AddKubeJobHandler<SendEmailJob, SendEmail>();
```

Business handlers only depend on job contracts. Runtime, storage and transport implementations remain isolated behind abstractions.

## Security

KubeJob HTTP and Dashboard endpoints require authorization by default. Configure
named client, worker, and dashboard policies (or a host default policy) and
call `UseAuthentication()` / `UseAuthorization()` before mapping controllers.
`AllowAnonymousEndpoints` and `AllowAnonymousAccess` are explicit local
development/test opt-outs only.

## Architecture

Core concepts:

- Job definition
- Execution runtime
- Worker execution engine
- Transport abstraction
- Storage abstraction

Supported transports can evolve independently:

```
Runtime
   |
Transport Abstraction
   |
+----------+----------+
|          |          |
RabbitMQ   Kafka    Other
```

## Development

The repository supports Docker Compose, podman compose and podman-compose development environments.

```bash
bash scripts/dev-stack.sh up
```

## Documentation

See `docs/v3` for:

- architecture
- runtime model
- jobs and events
- transport design
- operations
- performance tuning

## License

MIT
