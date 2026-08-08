using System.Text;
using KubeJob.Core.Queues;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// RabbitMQ data-plane options for BrokerNative jobs. The normal topology is
/// intentionally small: one direct exchange and one physical execution queue
/// per logical KubeJob queue, consumed competitively by all worker replicas.
/// Retry/DLQ topology is internal transport plumbing.
/// </summary>
public sealed class RabbitMqBrokerNativeOptions
{
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    /// <summary>
    /// Business-domain Job exchange, for example "order.jobs". Logical queue
    /// names are used as routing keys.
    /// </summary>
    public string ExchangeName { get; set; } = "kubejob.jobs";

    /// <summary>
    /// Prefix for physical execution queues. A logical queue "order.created"
    /// becomes "kubejob.order.created" by default.
    /// </summary>
    public string QueuePrefix { get; set; } = "kubejob";

    public ushort PrefetchCount { get; set; } = 64;

    /// <summary>
    /// RabbitMQ callback dispatch parallelism. Zero uses the worker's
    /// MaxConcurrentJobs. A process-wide semaphore still enforces the worker
    /// concurrency limit across all consumed logical queues.
    /// </summary>
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

        var longestGeneratedName = new[]
        {
            ExchangeName,
            GetRetryExchangeName(),
            GetRetryQueueName(),
            GetDeadLetterExchangeName(),
            GetDeadLetterQueueName(),
            GetQueueName(new string('q', 100))
        }.MaxBy(name => Encoding.UTF8.GetByteCount(name))!;

        if (Encoding.UTF8.GetByteCount(longestGeneratedName) >= 255)
        {
            throw new InvalidOperationException(
                "RabbitMQ BrokerNative topology names must be shorter than 255 UTF-8 bytes.");
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
}
