using KubeJob.Core.Execution;

namespace KubeJob.Core.Events;

/// <summary>
/// Strongly typed event identity. Topic is the business event stream/domain;
/// RoutingKey identifies the event type inside that topic.
/// </summary>
public readonly record struct EventKey<TEvent>(string Topic, string RoutingKey)
{
    public static EventKey<TEvent> Create(string topic, string routingKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        return new EventKey<TEvent>(topic.Trim(), routingKey.Trim());
    }

    public bool IsEmpty => string.IsNullOrWhiteSpace(Topic) || string.IsNullOrWhiteSpace(RoutingKey);
}

public sealed class EventPublishOptions
{
    /// <summary>
    /// Number of delivery attempts available independently to each subscription.
    /// </summary>
    public int MaxAttempts { get; init; } = 3;

    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(5);

    public string? IdempotencyKey { get; init; }

    public string? PartitionKey { get; init; }

    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    public void Validate()
    {
        if (MaxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxAttempts));
        }

        if (Timeout <= TimeSpan.Zero || Timeout > TimeSpan.FromDays(1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(Timeout),
                "Timeout must be positive and no more than one day.");
        }
    }
}

public sealed record EventHandle(string EventId);

public interface IEventBus
{
    ValueTask<EventHandle> PublishAsync<TEvent>(
        EventKey<TEvent> eventKey,
        TEvent @event,
        CancellationToken cancellationToken = default);

    ValueTask<EventHandle> PublishAsync<TEvent>(
        EventKey<TEvent> eventKey,
        TEvent @event,
        EventPublishOptions options,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Handler for one subscription to an event. Different subscriptions use
/// independent physical delivery streams and therefore each receive the event.
/// Multiple replicas of the same subscription compete for those deliveries.
/// </summary>
public interface IKubeEventHandler<TEvent>
{
    ValueTask HandleAsync(
        TEvent @event,
        EventExecutionContext context,
        CancellationToken cancellationToken);
}

public sealed class EventExecutionContext
{
    public required string EventId { get; init; }

    public required string Topic { get; init; }

    public required string RoutingKey { get; init; }

    public required string Subscription { get; init; }

    public required int AttemptNumber { get; init; }

    public required WorkerExecutionInfo Worker { get; init; }

    public required IServiceProvider ServiceProvider { get; init; }
}

/// <summary>
/// Logical subscription definition. A transport maps this to its native
/// consumer-group/queue topology. Subscription name, not worker identity,
/// defines the independent delivery stream.
/// </summary>
public sealed record EventSubscriptionDefinition(
    string Topic,
    string RoutingKey,
    string Subscription,
    string HandlerKey)
{
    public static string CreateHandlerKey(
        string topic,
        string routingKey,
        string subscription)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        return $"$event:{topic.Trim()}:{subscription.Trim()}:{routingKey.Trim()}";
    }
}
