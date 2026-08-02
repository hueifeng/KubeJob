using KubeJob.Core.Queues;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Runtime;

public sealed record QueueRoute(string Queue, DeliveryTarget Target);

public interface IQueueRouter
{
    QueueRoute Resolve(string logicalQueue);
}

/// <summary>
/// Resolves registered transport implementations by stable deployment ID. More
/// than one adapter may be registered in a host; routing chooses one target
/// per persisted Outbox row rather than relying on registration order.
/// </summary>
public interface IExecutionTransportRegistry
{
    IExecutionTransport Resolve(string transportId);
}

public sealed class ExecutionTransportRegistry : IExecutionTransportRegistry
{
    private readonly IReadOnlyDictionary<string, IExecutionTransport> _transports;

    public ExecutionTransportRegistry(IEnumerable<IExecutionTransport> transports)
    {
        ArgumentNullException.ThrowIfNull(transports);
        _transports = transports.ToDictionary(
            transport => transport.TransportId,
            StringComparer.Ordinal);
    }

    public IExecutionTransport Resolve(string transportId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(transportId);
        return _transports.TryGetValue(transportId, out var transport)
            ? transport
            : throw new InvalidOperationException(
                $"No KubeJob execution transport is registered with ID '{transportId}'.");
    }
}

/// <summary>
/// Delivery policy for one logical queue: the deployment-owned choices that
/// resolve a queue name into an execution target. One queue has one
/// definition, so the Dashboard and operator tooling can show a single
/// per-queue row instead of cross-referencing several configuration maps.
/// </summary>
public sealed class QueueDefinition
{
    /// <summary>How the queue's Runs are discovered: broker delivery or pull.</summary>
    public ExecutionDeliveryProfile Profile { get; set; } = ExecutionDeliveryProfile.BrokerDispatch;

    /// <summary>Per-queue ordering contract (Parallel, KeyOrdered, StrictFifo).</summary>
    public ExecutionOrderingMode OrderingMode { get; set; } = ExecutionOrderingMode.Parallel;

    /// <summary>Worker eligibility and isolation boundary for this queue.</summary>
    public string ExecutionLane { get; set; } = "default";

    /// <summary>Consumer group that serves this queue.</summary>
    public string ConsumerGroup { get; set; } = "default";

    /// <summary>Transport adapter that physically delivers this queue's Runs.</summary>
    public string? TransportId { get; set; } = "rabbitmq";
}

/// <summary>
/// Deployment-level queue routing policy. It intentionally has no relationship
/// to EnqueueJobRequest, so a business caller cannot choose a physical target
/// for an individual Run.
/// </summary>
public sealed class QueueDeliveryOptions
{
    /// <summary>Defaults applied to any queue without an explicit definition.</summary>
    public QueueDefinition Defaults { get; } = new();

    /// <summary>
    /// Explicit per-queue definitions, keyed by canonical logical queue name.
    /// A queue without an entry uses <see cref="Defaults"/>.
    /// </summary>
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

            if (trimmed.Length > 100)
            {
                throw new InvalidOperationException(
                    $"Queue policy contains an invalid logical queue (over 100 characters): '{trimmed}'.");
            }

            ValidateDefinition(trimmed, entry.Value);
        }
    }

    private static void ValidateDefinition(string queue, QueueDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        if (!Enum.IsDefined(definition.Profile))
        {
            throw new InvalidOperationException(
                $"Queue policy for '{queue}' has an unsupported execution profile.");
        }

        if (definition.Profile == ExecutionDeliveryProfile.BrokerDispatch
            && string.IsNullOrWhiteSpace(definition.TransportId))
        {
            throw new InvalidOperationException(
                $"Queue policy for '{queue}' uses BrokerDispatch but has no TransportId.");
        }

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
            definition.Profile,
            definition.ExecutionLane,
            definition.Profile == ExecutionDeliveryProfile.BrokerDispatch
                ? definition.TransportId
                : null,
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

public sealed class UnconfiguredExecutionTransport : IExecutionTransport
{
    private readonly ILogger<UnconfiguredExecutionTransport> _logger;

    public UnconfiguredExecutionTransport(ILogger<UnconfiguredExecutionTransport> logger)
    {
        _logger = logger;
    }

    public string TransportId => "unconfigured";

    public ValueTask PublishAsync(ExecutionEnvelope envelope, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(envelope);
        _logger.LogError(
            "Broker dispatch requires a registered execution transport, but the envelope for Run {RunId} (queue {Queue}) could not be routed to any adapter",
            envelope.RunId,
            envelope.Queue);
        throw new InvalidOperationException("No KubeJob execution transport is registered for broker dispatch.");
    }
}
