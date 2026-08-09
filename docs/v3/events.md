# Event subscriptions

BrokerNative events are published once and delivered to each named
subscription. A subscription is a durable consumer queue, not just a label in
application code.

```text
topic + routing key → exchange/topic → subscription queue → handler → ACK
```

## Register a subscription

The topic and routing key identify the event. The subscription name identifies
one independent delivery stream:

```csharp
var orderCreated = EventKey<OrderCreated>.Create(
    "orders",
    "order.created");

builder.Services.AddKubeJobEventHandler<OrderCreated, AuditOrderCreated>(
    orderCreated,
    subscription: "order-audit");
```

Every replica of the audit service should use `order-audit`; those replicas
compete for one queue. A separate service should use another name, such as
`order-search-index`, to receive its own copy.

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
handler fails, the broker retries that subscription. A terminal failure goes to
the subscription's dead-letter route; it is not republished to every other
subscriber.

This means each subscription can choose its own retry count, dead-letter
retention, and operational owner. KubeJob does not write an event history to
the managed job tables. If the application needs an audit trail, store the
event id and business identifiers in an application table or an observability
pipeline.

## Delivery semantics

Event delivery is at-least-once. A process can finish the handler and lose its
connection before the broker sees the acknowledgement, so the same event may
be delivered again. Make handlers idempotent with `EventId` plus a business
identifier, and keep external side effects behind a deduplication check when
necessary.
