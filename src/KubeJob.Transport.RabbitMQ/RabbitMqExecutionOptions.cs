using KubeJob.Core.Queues;
using System.Text;

namespace KubeJob.Transport.RabbitMQ;

public sealed class RabbitMqExecutionOptions
{
    public string TransportId { get; set; } = "rabbitmq";

    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    public string ConsumerGroup { get; set; } = "default";

    public string ConsumerQueuePrefix { get; set; } = "kubejob.execution";

    /// <summary>
    /// Number of physical execution lane queues per consumer group. A run's
    /// PartitionKey (the control-plane ConcurrencyKey) is hashed to a lane so
    /// same-key runs co-locate on one lane queue, reducing wasted broker Retry
    /// round-trips when the durable KeyOrdered claim gate blocks a later
    /// same-key run. The broker is never the ordering authority; the
    /// control-plane gate is unchanged. Default 1 is byte-for-byte identical
    /// to the single-queue topology (zero migration for existing deployments).
    /// </summary>
    public int ExecutionLaneCount { get; set; } = 1;

    /// <summary>
    /// Declares a per-worker cancel queue. Disabled by default because durable
    /// CancelRequested state plus lease renewal is the correctness path.
    /// </summary>
    public bool EnableCancelQueue { get; set; }

    public ushort PrefetchCount { get; set; } = 32;

    /// <summary>
    /// Maximum number of envelopes admitted in one control-plane claim
    /// transaction. The consumer collects up to this many deliveries before
    /// batch admission, so per-envelope admission round trips amortize to
    /// roughly two transactions per batch (one claim, one diagnostic read for
    /// unclaimed envelopes). Set <see cref="PrefetchCount"/> at least as large
    /// as this value so the collector can fill a full batch. A batch is only a
    /// delivery-rate optimization; every envelope is still admitted and
    /// executed individually with identical fencing and ordering semantics.
    /// </summary>
    public int AdmissionBatchSize { get; set; } = 16;

    /// <summary>
    /// Maximum number of RabbitMQ delivery callbacks dispatched concurrently
    /// on one connection. Zero uses the registered worker capacity. This is a
    /// transport throughput limit, not an ordering guarantee; KeyOrdered runs
    /// are still admitted by the durable control-plane gate.
    /// </summary>
    public ushort ConsumerDispatchConcurrency { get; set; }

    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// Maximum number of broker-only retry cycles before a pending Run is
    /// handed back to the durable Outbox reconciliation path.
    public int MaxBrokerRetryAttempts { get; set; } = 8;

    public TimeSpan BrokerRetryReconciliationDelay { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

    public int PublisherConcurrency { get; set; } = 8;

    /// <summary>
    /// The default is 0 (disabled): transient capacity and database failures
    /// must not strand a Pending Run in the broker DLQ before KubeJob can
    /// reconcile it. Set a positive value only when a deployment also has a
    /// DLQ re-drive policy for Pending Runs.
    /// </summary>
    public int DefaultDeliveryLimit { get; set; }

    /// <summary>
    /// When true, each execution lane queue is declared with Single Active
    /// Consumer (x-single-active-consumer). At most one consumer processes
    /// the lane at a time; failover triggers automatic consumer promotion on
    /// the standby node. Required for <see cref="Core.Runtime.ExecutionOrderingMode.StrictFifo"/>.
    /// </summary>
    public bool UseSingleActiveConsumer { get; set; }

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(TransportId);

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

        if (PublisherConcurrency is < 1 or > 32)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution PublisherConcurrency must be between 1 and 32.");
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

        if (ExecutionLaneCount is < 1 or > 64)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution ExecutionLaneCount must be between 1 and 64.");
        }

        // The lane suffix is empty for N=1 so this check is byte-identical to
        // the pre-lane maximum-name check; for N up to 64 it grows by at most
        // ".lane-63" (8 bytes), which still must fit under the 255-byte cap.
        var maxLaneSuffix = ExecutionLaneCount <= 1
            ? string.Empty
            : $".lane-{ExecutionLaneCount - 1}";
        var maximumQueueName = $"{ConsumerQueuePrefix}.{ConsumerGroup}.{new string('q', 48)}-ffffff{maxLaneSuffix}.queue";
        var sharedRetryQueueName = GetSharedRetryQueueName();
        var maximumCancelQueueName = $"{ConsumerQueuePrefix}.{ConsumerGroup}.cancel.{new string('q', 48)}-ffffff";
        var generatedNames = new[]
        {
            GetGroupExchangeName(),
            GetGroupDlxName(),
            GetGroupDlqName(),
            sharedRetryQueueName,
            GetCancelExchangeName(ConsumerGroup),
            maximumQueueName,
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

        if (AdmissionBatchSize is < 1 or > 256)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution AdmissionBatchSize must be between 1 and 256.");
        }

        if (ConsumerDispatchConcurrency > 256)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution ConsumerDispatchConcurrency cannot exceed 256.");
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

    internal string GetConsumerQueueName(string logicalQueue) => GetConsumerQueueName(logicalQueue, 0);

    /// <summary>
    /// Physical execution queue for a logical queue and lane. Each business
    /// logical queue owns its own durable dispatch queue; lane 0 has no suffix.
    /// Additional lanes are an explicit per-queue scaling choice.
    /// </summary>
    internal string GetConsumerQueueName(string logicalQueue, int lane)
    {
        logicalQueue = LogicalQueueName.Normalize(logicalQueue, nameof(logicalQueue));
        return GetLogicalQueueName(logicalQueue, LaneSuffix(lane));
    }

    internal string GetGroupExchangeName() => GetGroupExchangeName(ConsumerGroup);

    internal string GetGroupExchangeName(string consumerGroup)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        var normalizedGroup = consumerGroup.Trim();
        if (normalizedGroup.Length > 200)
        {
            throw new ArgumentException("Consumer groups cannot exceed 200 characters.", nameof(consumerGroup));
        }
        return $"{ConsumerQueuePrefix}.{normalizedGroup}";
    }

    internal string GetRetryExchangeName() =>
        $"{ConsumerQueuePrefix}.{ConsumerGroup}.retry";

    internal string GetSharedRetryQueueName() =>
        $"{ConsumerQueuePrefix}.{ConsumerGroup}.retry.queue";

    /// <summary>
    /// Lane-suffixed routing / binding key for a logical queue. When
    /// <see cref="ExecutionLaneCount"/> is 1 the routing key is the bare
    /// logical queue name (byte-identical to the pre-lane topology); for N&gt;1
    /// it is <c>{logicalQueue}.lane-{lane}</c>. Publishing, queue binding, and
    /// retry republish all use this key so a retried message re-lands on the
    /// same lane queue after the broker TTL dead-letter.
    /// </summary>
    internal string GetLaneRoutingKey(string logicalQueue, int lane)
    {
        logicalQueue = LogicalQueueName.Normalize(logicalQueue, nameof(logicalQueue));
        return ExecutionLaneCount <= 1 ? logicalQueue : $"{logicalQueue}.lane-{lane}";
    }

    private string LaneSuffix(int lane) =>
        ExecutionLaneCount <= 1 ? string.Empty : $".lane-{lane}";

    private string GetLogicalQueueName(string logicalQueue, string laneSuffix)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        var name = $"{ConsumerQueuePrefix}.{ConsumerGroup}.{logicalQueue}{laneSuffix}.queue";
        if (Encoding.UTF8.GetByteCount(name) >= 255)
        {
            throw new InvalidOperationException(
                $"RabbitMQ execution queue names must be shorter than 255 UTF-8 bytes; got '{name}'.");
        }

        return name;
    }

    internal string GetGroupDlxName() =>
        $"{ConsumerQueuePrefix}.{ConsumerGroup}.dlx";

    internal string GetGroupDlqName() =>
        $"{ConsumerQueuePrefix}.{ConsumerGroup}.dlq.queue";

    internal string GetCancelExchangeName(string group) =>
        $"{ConsumerQueuePrefix}.{group}.cancel";

    internal string GetCancelQueueName(string group, string workerIdentity) =>
        $"{ConsumerQueuePrefix}.{group}.cancel.{SanitizeWorkerIdentity(workerIdentity)}";

    /// <summary>
    /// Sanitizes an opaque worker identity into a RabbitMQ-safe segment:
    /// lower-cased alnum + dash, collapsed repeats, trimmed dashes, capped at
    /// 48 chars, and always suffixed with a stable 6-character hash so distinct
    /// worker identities cannot collapse to one physical cancel queue. Business
    /// logical queues intentionally keep their literal names in durable queue
    /// topology; see <see cref="GetLogicalQueueName"/>.
    /// </summary>
    internal static string SanitizeWorkerIdentity(string value)
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
