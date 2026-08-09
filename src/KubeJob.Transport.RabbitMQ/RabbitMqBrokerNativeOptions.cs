using System.Text;
using KubeJob.Core.Queues;
using KubeJob.Core.Runtime;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// RabbitMQ data-plane options for BrokerNative jobs and events. Jobs use one
/// direct exchange plus one execution queue per logical KubeJob Queue. Events
/// use the fixed business exchange and three capability queues: log, data, and
/// notify. Retry/DLQ topology remains internal transport plumbing.
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

    /// <summary>Prefix for physical Job execution queues.</summary>
    public string QueuePrefix { get; set; } = "kubejob";

    /// <summary>Business event exchange shared by all event types.</summary>
    public string EventExchangeName { get; set; } = "order.exchange";

    /// <summary>Fixed capability queue for logging event consumers.</summary>
    public string LogEventQueueName { get; set; } = "log.queue";

    /// <summary>Fixed capability queue for data event consumers.</summary>
    public string DataEventQueueName { get; set; } = "data.queue";

    /// <summary>Fixed capability queue for notification event consumers.</summary>
    public string NotifyEventQueueName { get; set; } = "notify.queue";

    public ushort PrefetchCount { get; set; } = 64;

    public ushort ConsumerDispatchConcurrency { get; set; }

    /// <summary>
    /// Fixed RabbitMQ BrokerNative retry delay. Each job retry queue and each
    /// event-subscription retry queue uses this value as its queue-level TTL.
    /// Per-message RetryPolicy may still control the retry budget/intent at the
    /// runtime model level, but RabbitMQ deliberately does not create variable
    /// delay queues or per-message expirations.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Compatibility helper for callers that need to represent the RabbitMQ
    /// adapter's fixed retry delay as a generic RetryPolicy.
    /// </summary>
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
        ArgumentException.ThrowIfNullOrWhiteSpace(EventExchangeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(LogEventQueueName);
        ArgumentException.ThrowIfNullOrWhiteSpace(DataEventQueueName);
        ArgumentException.ThrowIfNullOrWhiteSpace(NotifyEventQueueName);

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
        ValidateTopologyName(EventExchangeName, nameof(EventExchangeName));
        ValidateTopologyName(LogEventQueueName, nameof(LogEventQueueName));
        ValidateTopologyName(DataEventQueueName, nameof(DataEventQueueName));
        ValidateTopologyName(NotifyEventQueueName, nameof(NotifyEventQueueName));
        _ = GetRetryExchangeName();
        _ = GetRetryQueueName();
        _ = GetDeadLetterExchangeName();
        _ = GetDeadLetterQueueName();
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
        _ = LogicalQueueName.Normalize(topic, nameof(topic));
        return EventExchangeName;
    }

    public string GetEventSubscriptionQueueName(string topic, string subscription)
    {
        _ = LogicalQueueName.Normalize(topic, nameof(topic));
        return LogicalQueueName.Normalize(subscription, nameof(subscription)) switch
        {
            "log" => LogEventQueueName,
            "data" => DataEventQueueName,
            "notify" => NotifyEventQueueName,
            _ => throw new InvalidOperationException(
                "RabbitMQ event subscriptions must target one of the fixed capability queues: log, data, or notify.")
        };
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
