using System.Text;

namespace KubeJob.Transport.RabbitMQ;

public sealed class RabbitMqNotificationOptions
{
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    public string ExchangeName { get; set; } = "kubejob.work-available";

    /// <summary>
    /// Workers with the same group compete for Queue wake-up messages. Use
    /// different groups only when independent worker pools must each be woken.
    /// </summary>
    public string ConsumerGroup { get; set; } = "default";

    public string ConsumerQueuePrefix { get; set; } = "kubejob.work-available";

    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

    public void Validate()
    {
        if (!Uri.TryCreate(ConnectionString, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("amqp" or "amqps"))
        {
            throw new InvalidOperationException(
                "RabbitMQ ConnectionString must be an absolute amqp or amqps URI.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(ExchangeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(ConsumerGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(ConsumerQueuePrefix);
        if (Encoding.UTF8.GetByteCount(ExchangeName) >= 255)
        {
            throw new InvalidOperationException("ExchangeName must be shorter than 255 UTF-8 bytes.");
        }

        if (ConsumerGroup.Length > 200)
        {
            throw new InvalidOperationException("ConsumerGroup cannot exceed 200 characters.");
        }

        if (Encoding.UTF8.GetByteCount(ConsumerQueuePrefix) > 180)
        {
            throw new InvalidOperationException(
                "ConsumerQueuePrefix cannot exceed 180 UTF-8 bytes.");
        }

        var maximumQueueName = $"{ConsumerQueuePrefix}.{ConsumerGroup}.{new string('q', 48)}-ffffff";
        if (Encoding.UTF8.GetByteCount(maximumQueueName) >= 255)
        {
            throw new InvalidOperationException(
                "Generated notification queue names must be shorter than 255 UTF-8 bytes.");
        }

        if (ReconnectDelay <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("ReconnectDelay must be positive.");
        }

        if (PublisherConfirmTimeout <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("PublisherConfirmTimeout must be positive.");
        }
    }

    internal string GetConsumerQueueName(string logicalQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        var segment = RabbitMqExecutionOptions.SanitizeSegment(logicalQueue);
        return $"{ConsumerQueuePrefix}.{ConsumerGroup}.{segment}";
    }
}
