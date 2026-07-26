using System.Text;
using System.Security.Cryptography;

namespace KubeJob.Transport.RabbitMQ;

public sealed class RabbitMqExecutionOptions
{
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    /// <summary>
    /// Legacy direct exchange name. Execution dispatch now publishes to the
    /// per-group direct exchange (<see cref="GetGroupExchangeName"/>) that
    /// <c>RabbitMqDispatchTopology</c> binds each quorum queue to, so this
    /// value is no longer used for execution dispatch. Retained to avoid
    /// breaking existing option configurations; will be removed in a future
    /// version.
    /// </summary>
    [Obsolete("Execution dispatch publishes to the per-group direct exchange (GetGroupExchangeName). This value is no longer used for dispatch and will be removed in a future version.")]
    public string ExchangeName { get; set; } = "kubejob.execution";

    public string ConsumerGroup { get; set; } = "default";

    public string ConsumerQueuePrefix { get; set; } = "kubejob.execution";

    public ushort PrefetchCount { get; set; } = 16;

    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// Maximum number of broker-only retry cycles before a pending Run is
    /// handed back to the durable Outbox reconciliation path.
    public int MaxBrokerRetryAttempts { get; set; } = 8;

    public TimeSpan BrokerRetryReconciliationDelay { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The default is 0 (disabled): transient capacity and database failures
    /// must not strand a Pending Run in the broker DLQ before KubeJob can
    /// reconcile it. Set a positive value only when a deployment also has a
    /// DLQ re-drive policy for Pending Runs.
    /// </summary>
    public int DefaultDeliveryLimit { get; set; }

    public void Validate()
    {
        if (!Uri.TryCreate(ConnectionString, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("amqp" or "amqps"))
        {
            throw new InvalidOperationException(
                "RabbitMQ execution ConnectionString must be an absolute amqp or amqps URI.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(ConsumerGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(ConsumerQueuePrefix);

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

        var maximumQueueName = $"{ConsumerQueuePrefix}.{ConsumerGroup}.{new string('q', 48)}-ffffff";
        var maximumRetryQueueName = $"{ConsumerQueuePrefix}.{ConsumerGroup}.retry.{new string('q', 48)}-ffffff";
        var maximumCancelQueueName = $"{ConsumerQueuePrefix}.{ConsumerGroup}.cancel.{new string('q', 48)}-ffffff";
        var generatedNames = new[]
        {
            GetGroupExchangeName(),
            GetGroupDlxName(),
            GetGroupDlqName(),
            GetCancelExchangeName(ConsumerGroup),
            GetCancelQueueName(ConsumerGroup),
            maximumQueueName,
            maximumRetryQueueName,
            maximumCancelQueueName
        };
        if (generatedNames.Any(name => Encoding.UTF8.GetByteCount(name) >= 255))
        {
            throw new InvalidOperationException(
                "RabbitMQ execution topology names must be shorter than 255 UTF-8 bytes after composition.");
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

        if (MaxBrokerRetryAttempts is < 1 or > 1_000)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution MaxBrokerRetryAttempts must be between 1 and 1000.");
        }

        if (BrokerRetryReconciliationDelay <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution BrokerRetryReconciliationDelay must be positive.");
        }

        if (RetryDelay.TotalMilliseconds > int.MaxValue)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution RetryDelay cannot exceed the broker TTL integer range.");
        }

        if (DefaultDeliveryLimit < 0)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution DefaultDeliveryLimit cannot be negative.");
        }
    }

    internal string GetConsumerQueueName(string logicalQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        var segment = SanitizeSegment(logicalQueue);
        return $"{ConsumerQueuePrefix}.{ConsumerGroup}.{segment}";
    }

    internal string GetConsumerQueueDlqName(string logicalQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        var segment = SanitizeSegment(logicalQueue);
        return $"{ConsumerQueuePrefix}.{ConsumerGroup}.{segment}.dlq";
    }

    internal string GetGroupExchangeName() =>
        $"{ConsumerQueuePrefix}.{ConsumerGroup}";

    internal string GetRetryExchangeName() =>
        $"{ConsumerQueuePrefix}.{ConsumerGroup}.retry";

    internal string GetRetryQueueName(string logicalQueue) =>
        $"{ConsumerQueuePrefix}.{ConsumerGroup}.retry.{SanitizeSegment(logicalQueue)}";

    internal string GetGroupDlxName() =>
        $"{ConsumerQueuePrefix}.{ConsumerGroup}.dlx";

    internal string GetGroupDlqName() =>
        $"{ConsumerQueuePrefix}.{ConsumerGroup}.dlq";

    internal string GetCancelExchangeName(string group) =>
        $"{ConsumerQueuePrefix}.{group}.cancel";

    internal string GetCancelQueueName(string group) =>
        $"{ConsumerQueuePrefix}.{group}.cancel.workers";

    internal string GetCancelQueueName(string group, string workerIdentity) =>
        $"{ConsumerQueuePrefix}.{group}.cancel.{SanitizeSegment(workerIdentity)}";

    /// <summary>
    /// Sanitizes a logical KubeJob queue name into a RabbitMQ-safe segment:
    /// lower-cased alnum + dash, collapsed repeats, trimmed dashes, capped at
    /// 48 chars, and always suffixed with a stable 6-character hash so distinct
    /// logical queue names cannot collapse to one physical queue.
    /// </summary>
    internal static string SanitizeSegment(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var lower = value.ToLowerInvariant();
        var buffer = new char[lower.Length];
        var length = 0;
        var lastWrittenDash = true;
        foreach (var ch in lower)
        {
            var safe = (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9');
            if (safe)
            {
                buffer[length++] = ch;
                lastWrittenDash = false;
            }
            else if (!lastWrittenDash)
            {
                buffer[length++] = '-';
                lastWrittenDash = true;
            }
        }
        if (length > 0 && buffer[length - 1] == '-')
        {
            length--;
        }

        var digest = Convert.ToHexString(
                System.Security.Cryptography.SHA256.HashData(
                    System.Text.Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();
        var prefix = length == 0
            ? "queue"
            : new string(buffer, 0, Math.Min(length, 48));
        return $"{prefix}-{digest[..6]}";
    }
}
