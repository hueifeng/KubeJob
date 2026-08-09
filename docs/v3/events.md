# Event subscriptions

BrokerNative events are published to the shared `order.exchange` and delivered
to the fixed `log.queue`, `data.queue`, and `notify.queue` capability queues.

```text
topic + routing key → order.exchange → capability queue → handler → ACK
```

## Register a subscription

The topic and routing key identify the event. The subscription selects one of
the fixed capability queues: `log`, `data`, or `notify`.

```csharp
var orderCreated = EventKey<OrderCreated>.Create(
    "orders",
    "order.created");

builder.Services.AddKubeJobEventHandler<OrderCreated, AuditOrderCreated>(
    orderCreated,
    subscription: "log");
```

Every replica using `log` competes for `log.queue`. Use `data` or `notify`
when a separate capability needs its own copy; arbitrary subscription queues
are intentionally not created.

Map the topic to a transport and start the event consumer:

```csharp
builder.Services.AddKubeJobServer();
builder.Services.ConfigureKubeJobEventRuntimes(options =>
    options.Topics["orders"] = RabbitMqBrokerNativePublisher.Id);
builder.Services.AddRabbitMqKubeJobEventConsumer(options =>
    options.ConnectionString = rabbitMqConnectionString);
```

## Retry and dead letters

An acknowledgement is sent only after the handler returns successfully. If a
handler fails, the broker retries that capability queue. A terminal failure
goes to that queue's dead-letter route; it is not republished to every other
capability queue.

KubeJob does not write an event history to
the managed job tables. If the application needs an audit trail, store the
event id and business identifiers in an application table or an observability
pipeline.

`EventPublishOptions` accepts `MaxAttempts` and an optional `RetryPolicy`:

```csharp
await eventBus.PublishAsync(
    orderCreated,
    payload,
    new EventPublishOptions
    {
        MaxAttempts = 5,
        RetryPolicy = new RetryPolicy(
            BackoffStrategy.Exponential,
            TimeSpan.FromSeconds(2),
            TimeSpan.FromMinutes(2))
    });
```

The policy is copied into the self-contained BrokerNative envelope and is
preserved on retry. Older envelopes without a policy use the RabbitMQ
transport's fixed `RetryDelay`. RabbitMQ's retry queue also has a queue-level
TTL for compatibility, so configure `RetryDelay` at least as high as the
largest custom policy delay until the retry topology is migrated.

## Delivery semantics

Event delivery is at-least-once. A process can finish the handler and lose its
connection before the broker sees the acknowledgement, so the same event may
be delivered again. Make handlers idempotent with `EventId` plus a business
identifier, and keep external side effects behind a deduplication check when
necessary.
