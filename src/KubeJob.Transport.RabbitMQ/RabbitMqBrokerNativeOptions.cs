using System.Text;
using KubeJob.Core.Queues;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// RabbitMQ data-plane options for BrokerNative jobs and events. Jobs use one
/// direct exchange plus one execution queue per logical KubeJob Queue. Events
/// use one topic exchange per logical Topic and one queue per Subscription.
/// Retry/DLQ topology remains internal transport plumbing.
/// </summary>
public sealed class RabbitMqBrokerNativeOptions
{
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

    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

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

        var maximumLogicalName = new string('q', LogicalQueueName.MaximumLength);
        var longestGeneratedName = new[]
        {
            ExchangeName,
            GetRetryExchangeName(),
            GetRetryQueueName(),
            GetDeadLetterExchangeName(),
            GetDeadLetterQueueName(),
            GetQueueName(maximumLogicalName),
            GetEventExchangeName(maximumLogicalName),
            GetEventSubscriptionQueueName(maximumLogicalName, maximumLogicalName),
            GetEventRetryExchangeName(maximumLogicalName),
            GetEventRetryQueueName(maximumLogicalName, maximumLogicalName),
            GetEventDeadLetterExchangeName(maximumLogicalName),
            GetEventDeadLetterQueueName(maximumLogicalName, maximumLogicalName)
        }.MaxBy(name => Encoding.UTF8.GetByteCount(name))!;

        if (Encoding.UTF8.GetByteCount(longestGeneratedName) >= 255)
        {
            throw new InvalidOperationException(
                "RabbitMQ BrokerNative topology names must be shorter than 255 UTF-8 bytes. " +
                "Shorten QueuePrefix when using long Topic/Subscription names.");
        }
    }

    public string GetQueueName(string logicalQueue)
    {
        var queue = LogicalQueueName.Normalize(logicalQueue, nameof(logicalQueue));
        return $"{QueuePrefix}.{queue}";
    }

    public string GetRetryExchangeName() => $"{ExchangeName}.retry";

    public string GetRetryQueueName() => $"{ExchangeName}.retry.queue";

    public string GetDeadLetterExchangeName() => $"{ExchangeName}.dlx";

    public string GetDeadLetterQueueName() => $"{ExchangeName}.dlq";

    public string GetEventExchangeName(string topic)
    {
        var normalized = LogicalQueueName.Normalize(topic, nameof(topic));
        return $"{QueuePrefix}.{normalized}";
    }

    public string GetEventSubscriptionQueueName(string topic, string subscription)
    {
        var normalizedTopic = LogicalQueueName.Normalize(topic, nameof(topic));
        var normalizedSubscription = LogicalQueueName.Normalize(subscription, nameof(subscription));
        return $"{QueuePrefix}.{normalizedTopic}.{normalizedSubscription}";
    }

    public string GetEventRetryExchangeName(string topic)
        => $"{GetEventExchangeName(topic)}.retry";

    public string GetEventRetryQueueName(string topic, string subscription)
        => $"{GetEventSubscriptionQueueName(topic, subscription)}.retry";

    public string GetEventDeadLetterExchangeName(string topic)
        => $"{GetEventExchangeName(topic)}.dlx";

    public string GetEventDeadLetterQueueName(string topic, string subscription)
        => $"{GetEventSubscriptionQueueName(topic, subscription)}.dlq";
}
