using KubeJob.Core.Queues;

namespace KubeJob.Core.Runtime;

/// <summary>
/// Selects the single execution authority for a logical Queue.
/// </summary>
public enum QueueRuntimeMode
{
    /// <summary>
    /// PostgreSQL owns Run/Attempt/Lease/Fencing and is the Queue authority.
    /// </summary>
    PostgresManaged = 0,

    /// <summary>
    /// The configured message transport owns delivery, retry and redelivery.
    /// PostgreSQL is not part of the normal publish/consume hot path.
    /// </summary>
    BrokerNative = 1
}

public sealed class QueueRuntimeRoute
{
    public QueueRuntimeMode Mode { get; set; } = QueueRuntimeMode.PostgresManaged;

    /// <summary>
    /// Transport adapter id used only by BrokerNative routes, for example
    /// "rabbitmq", "kafka" or "sqs".
    /// </summary>
    public string? TransportId { get; set; }

    public QueueRuntimeRoute Clone() => new()
    {
        Mode = Mode,
        TransportId = TransportId
    };
}

/// <summary>
/// Deployment-level Queue authority configuration. Runtime mode is deliberately
/// resolved from Queue configuration rather than carried by each job message,
/// so callers cannot switch source-of-truth semantics per submission.
/// </summary>
public sealed class QueueRuntimeOptions
{
    public QueueRuntimeMode DefaultMode { get; set; } = QueueRuntimeMode.PostgresManaged;

    public string DefaultTransportId { get; set; } = "rabbitmq";

    public Dictionary<string, QueueRuntimeRoute> Queues { get; } =
        new(StringComparer.Ordinal);

    public QueueRuntimeRoute Resolve(string logicalQueue)
    {
        var queue = LogicalQueueName.Normalize(logicalQueue, nameof(logicalQueue));
        var route = Queues.TryGetValue(queue, out var configured)
            ? configured.Clone()
            : new QueueRuntimeRoute { Mode = DefaultMode };

        if (route.Mode == QueueRuntimeMode.BrokerNative)
        {
            route.TransportId = string.IsNullOrWhiteSpace(route.TransportId)
                ? DefaultTransportId.Trim()
                : route.TransportId.Trim();
            ArgumentException.ThrowIfNullOrWhiteSpace(route.TransportId);
        }
        else
        {
            route.TransportId = null;
        }

        return route;
    }
}

public interface IQueueRuntimeResolver
{
    QueueRuntimeRoute Resolve(string logicalQueue);
}
