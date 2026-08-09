using System.Text;
using KubeJob.Core.Queues;
using KubeJob.Core.Runtime;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// RabbitMQ data-plane options for BrokerNative jobs and events. Jobs use one
/// direct exchange plus one execution queue per logical KubeJob Queue. Events
/// use one topic exchange per logical Topic and one queue per Subscription.
/// Retry/DLQ topology remains internal transport plumbing.
/// </summary>
public sealed class RabbitMqBrokerNativeOptions
{
    private const int RabbitMqNameByteLimit = 255;

    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    /// <summary>
    /// Business-domain Job exchange. Logical Queue names are routing keys.
    /// </summary>
    public string ExchangeName { get; set; } = "kubejob.jobs";

    /// <summary>
    /// Prefix for physical execution queues and Event topic exchanges.
    /// A logical queue "order.created" becomes "kubejob.order.created";
    /// topic "order.events" becomes exchange "kubejob.order.events".
    /// </summary>
    public string QueuePrefix { get; set; } = "kubejob";

    public ushort PrefetchCount { get; set; } = 64;

    public ushort ConsumerDispatchConcurrency { get; set; }

    /// <summary>
    /// Fallback retry delay for messages without a policy. It is also the
    /// queue-level safety TTL, so set it at least as high as custom policies'
    /// maximum delay until the retry queue topology is migrated.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

    public RetryPolicy GetFallbackRetryPolicy() =>
        new(BackoffStrategy.Fixed, RetryDelay, RetryDelay);

    public void Validate()
    {
        if (!Uri.TryCreate(ConnectionString, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("amqp" or "amqps"))
        {
            throw new InvalidOperationException(
                "RabbitMQ BrokerNative ConnectionString must be an absolute amqp or amqps URI.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(ExchangeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(QueuePrefix);

        if (PrefetchCount == 0)
        {
            throw new InvalidOperationException("RabbitMQ BrokerNative PrefetchCount must be positive.");
        }

        if (ConsumerDispatchConcurrency > 256)
        {
            throw new InvalidOperationException(
                "RabbitMQ BrokerNative ConsumerDispatchConcurrency cannot exceed 256.");
        }

        if (RetryDelay <= TimeSpan.Zero || RetryDelay.TotalMilliseconds > int.MaxValue)
        {
            throw new InvalidOperationException(
                "RabbitMQ BrokerNative RetryDelay must be positive and fit the broker TTL integer range.");
        }

        if (ReconnectDelay <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("RabbitMQ BrokerNative ReconnectDelay must be positive.");
        }

        if (PublisherConfirmTimeout <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "RabbitMQ BrokerNative PublisherConfirmTimeout must be positive.");
        }

        ValidateTopologyName(ExchangeName, nameof(ExchangeName));
        _ = GetRetryExchangeName();
        _ = GetRetryQueueName();
        _ = GetDeadLetterExchangeName();
        _ = GetDeadLetterQueueName();

        // Logical queue/topic/subscription names are validated when a concrete
        // topology name is generated. Validating the theoretical combination
        // of two maximum-length logical names here would reject every default
        // configuration even when the application only uses short names.
    }

    public string GetQueueName(string logicalQueue)
    {
        var queue = LogicalQueueName.Normalize(logicalQueue, nameof(logicalQueue));
        return ValidateTopologyName($"{QueuePrefix}.{queue}", "job queue");
    }

    public string GetRetryExchangeName()
        => ValidateTopologyName($"{ExchangeName}.retry", "job retry exchange");

    public string GetRetryQueueName()
        => ValidateTopologyName($"{ExchangeName}.retry.queue", "job retry queue");

    public string GetDeadLetterExchangeName()
        => ValidateTopologyName($"{ExchangeName}.dlx", "job dead-letter exchange");

    public string GetDeadLetterQueueName()
        => ValidateTopologyName($"{ExchangeName}.dlq", "job dead-letter queue");

    public string GetEventExchangeName(string topic)
    {
        var normalized = LogicalQueueName.Normalize(topic, nameof(topic));
        return ValidateTopologyName($"{QueuePrefix}.{normalized}", "event exchange");
    }

    public string GetEventSubscriptionQueueName(string topic, string subscription)
    {
        var normalizedTopic = LogicalQueueName.Normalize(topic, nameof(topic));
        var normalizedSubscription = LogicalQueueName.Normalize(subscription, nameof(subscription));
        return ValidateTopologyName(
            $"{QueuePrefix}.{normalizedTopic}.{normalizedSubscription}",
            "event subscription queue");
    }

    public string GetEventRetryExchangeName(string topic)
        => ValidateTopologyName($"{GetEventExchangeName(topic)}.retry", "event retry exchange");

    public string GetEventRetryQueueName(string topic, string subscription)
        => ValidateTopologyName(
            $"{GetEventSubscriptionQueueName(topic, subscription)}.retry",
            "event retry queue");

    public string GetEventDeadLetterExchangeName(string topic)
        => ValidateTopologyName($"{GetEventExchangeName(topic)}.dlx", "event dead-letter exchange");

    public string GetEventDeadLetterQueueName(string topic, string subscription)
        => ValidateTopologyName(
            $"{GetEventSubscriptionQueueName(topic, subscription)}.dlq",
            "event dead-letter queue");

    private static string ValidateTopologyName(string name, string kind)
    {
        if (Encoding.UTF8.GetByteCount(name) >= RabbitMqNameByteLimit)
        {
            throw new InvalidOperationException(
                $"RabbitMQ {kind} name must be shorter than {RabbitMqNameByteLimit} UTF-8 bytes. " +
                "Shorten QueuePrefix or the logical queue/topic/subscription name.");
        }

        return name;
    }
}
