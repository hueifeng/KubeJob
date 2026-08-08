using KubeJob.Core.Queues;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.ControlPlane.Runtime;

public sealed record QueueRoute(string Queue, DeliveryTarget Target);

public interface IQueueRouter
{
    QueueRoute Resolve(string logicalQueue);
}

/// <summary>
/// PostgresManaged queue policy. Execution authority is always PostgreSQL;
/// these settings only control managed worker eligibility and ordering.
/// BrokerNative authority/transport selection lives in QueueRuntimeOptions.
/// </summary>
public sealed class QueueDefinition
{
    public ExecutionOrderingMode OrderingMode { get; set; } = ExecutionOrderingMode.Parallel;

    /// <summary>
    /// Managed-only worker eligibility boundary retained for storage/schema
    /// compatibility. Broker transports must not turn this into physical queues.
    /// </summary>
    public string ExecutionLane { get; set; } = "default";

    public string ConsumerGroup { get; set; } = "default";
}

public sealed class QueueDeliveryOptions
{
    public QueueDefinition Defaults { get; } = new();

    public Dictionary<string, QueueDefinition> Queues { get; } = new(StringComparer.Ordinal);

    public void Validate()
    {
        ValidateDefinition("default", Defaults);

        foreach (var entry in Queues.ToArray())
        {
            var trimmed = entry.Key.Trim();
            if (trimmed.Length == 0)
            {
                throw new InvalidOperationException(
                    "Queue policy contains an empty logical queue identifier.");
            }

            string normalized;
            try
            {
                normalized = LogicalQueueName.Normalize(trimmed, nameof(Queues));
            }
            catch (ArgumentException exception)
            {
                throw new InvalidOperationException(
                    $"Queue policy contains an invalid logical queue key '{entry.Key}'.",
                    exception);
            }

            if (!string.Equals(entry.Key, normalized, StringComparison.Ordinal))
            {
                Queues.Remove(entry.Key);
                Queues[normalized] = entry.Value;
            }

            ValidateDefinition(normalized, entry.Value);
        }
    }

    private static void ValidateDefinition(string queue, QueueDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        if (!Enum.IsDefined(definition.OrderingMode))
        {
            throw new InvalidOperationException(
                $"Queue policy for '{queue}' has an invalid execution ordering mode.");
        }

        if (string.IsNullOrWhiteSpace(definition.ExecutionLane) || definition.ExecutionLane.Length > 200)
        {
            throw new InvalidOperationException(
                $"Queue policy for '{queue}' has an invalid execution lane.");
        }

        if (string.IsNullOrWhiteSpace(definition.ConsumerGroup) || definition.ConsumerGroup.Length > 200)
        {
            throw new InvalidOperationException(
                $"Queue policy for '{queue}' has an invalid consumer group.");
        }
    }
}

public sealed class QueueCatalog
{
    private readonly IOptions<QueueDeliveryOptions> _options;

    public QueueCatalog(IOptions<QueueDeliveryOptions> options)
    {
        _options = options;
    }

    public QueueRoute Resolve(string logicalQueue)
    {
        logicalQueue = LogicalQueueName.Normalize(logicalQueue, nameof(logicalQueue));
        var options = _options.Value;
        options.Validate();
        var definition = options.Queues.TryGetValue(logicalQueue, out var configured)
            ? configured
            : options.Defaults;

        var target = new DeliveryTarget(
            ExecutionDeliveryProfile.Pull,
            definition.ExecutionLane,
            null,
            definition.ConsumerGroup,
            definition.OrderingMode);
        target.Validate();
        return new QueueRoute(logicalQueue, target);
    }

    public IReadOnlyList<string> NormalizeWorkerQueues(IEnumerable<string> queues)
    {
        ArgumentNullException.ThrowIfNull(queues);
        return queues
            .Select(queue => LogicalQueueName.Normalize(queue, nameof(queues)))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(queue => queue, StringComparer.Ordinal)
            .ToArray();
    }
}

public sealed class ConfigurationQueueRouter : IQueueRouter
{
    private readonly QueueCatalog _catalog;

    public ConfigurationQueueRouter(IOptions<QueueDeliveryOptions> options)
    {
        _catalog = new QueueCatalog(options);
    }

    public QueueRoute Resolve(string logicalQueue) => _catalog.Resolve(logicalQueue);
}
