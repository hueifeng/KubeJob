# BrokerNative event subscriptions

Events are transport-native broadcasts. KubeJob publishes one event message to
a logical Topic; each named subscription receives an independent delivery path.

```text
Topic → broker exchange/topic → subscription queue → handler → ACK
```

Register a handler with a stable topic, routing key and subscription name:

```csharp
var orderCreated = EventKey<OrderCreated>.Create("orders", "order.created");
builder.Services.AddKubeJobEventHandler<OrderCreated, AuditOrderCreated>(
    orderCreated,
    subscription: "order-audit");
```

## Isolation and retries

- Each subscription has its own physical consumer queue.
- A failure retries only the failing subscription; it does not republish the
  business event to every subscriber.
- A terminal failure goes to that subscription's dead-letter path.
- ACK happens only after the handler succeeds or KubeJob has durably handed off
  its retry/dead-letter action.

Event delivery is at-least-once. Treat `EventId` and your business identifiers
as de-duplication inputs in downstream handlers.

## Configuration

Map logical topics to a registered transport adapter:

```csharp
builder.Services.ConfigureKubeJobEventRuntimes(options =>
    options.Topics["orders"] = RabbitMqBrokerNativePublisher.Id);
```

The publisher never creates a managed JobRun. Event history and business
projections belong to the consuming application or a dedicated observability
pipeline.
