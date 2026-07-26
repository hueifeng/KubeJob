using System.Text;
using System.Security.Cryptography;

namespace KubeJob.Transport.RabbitMQ;

public sealed class RabbitMqExecutionOptions
{
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    public string ExchangeName { get; set; } = "kubejob.execution";

    public string ConsumerGroup { get; set; } = "default";

    public string ConsumerQueuePrefix { get; set; } = "kubejob.execution";

    public ushort PrefetchCount { get; set; } = 16;

    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

    public void Validate()
    {
        if (!Uri.TryCreate(ConnectionString, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("amqp" or "amqps"))
        {
            throw new InvalidOperationException(
                "RabbitMQ execution ConnectionString must be an absolute amqp or amqps URI.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(ExchangeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(ConsumerGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(ConsumerQueuePrefix);
        if (Encoding.UTF8.GetByteCount(ExchangeName) >= 255)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution ExchangeName must be shorter than 255 UTF-8 bytes.");
        }

        if (PublisherConfirmTimeout <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution PublisherConfirmTimeout must be positive.");
        }

        if (ConsumerGroup.Length > 200)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution ConsumerGroup cannot exceed 200 characters.");
        }

        if (Encoding.UTF8.GetByteCount(ConsumerQueuePrefix) > 180)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution ConsumerQueuePrefix cannot exceed 180 UTF-8 bytes.");
        }

        if (PrefetchCount == 0)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution PrefetchCount must be positive.");
        }

        if (ReconnectDelay <= TimeSpan.Zero || RetryDelay <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution reconnect and retry delays must be positive.");
        }
    }

    internal string GetConsumerQueueName(string logicalQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        var identity = $"{ExchangeName}\n{ConsumerGroup}\n{logicalQueue}";
        var digest = Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(identity)))
            .ToLowerInvariant();
        return $"{ConsumerQueuePrefix}.{digest}";
    }
}
