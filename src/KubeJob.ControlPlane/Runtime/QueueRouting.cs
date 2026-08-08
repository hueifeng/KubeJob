using KubeJob.Core.Queues;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.ControlPlane.Runtime;

public sealed record QueueRoute(string Queue, DeliveryTarget Target);

public interface IQueueRouter
{
    QueueRoute Resolve(string logicalQueue);
}

/// <summary>
/// Legacy V2 execution-envelope transport registry. Kept temporarily for
/// explicitly configured BrokerDispatch queues while V3 BrokerNative replaces
/// that path. New broker-authoritative queues use IMessageTransportRegistry.
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
/// Legacy V2 delivery policy used only inside PostgresManaged compatibility
/// paths. The default is now pure PostgreSQL Pull so Managed has a single
/// execution authority. BrokerDispatch must be opted into explicitly.
/// </summary>
public sealed class QueueDefinition
{
    public ExecutionDeliveryProfile Profile { get; set; } = ExecutionDeliveryProfile.Pull;

    public ExecutionOrderingMode OrderingMode { get; set; } = ExecutionOrderingMode.Parallel;

    public string ExecutionLane { get; set; } = "default";

    public string ConsumerGroup { get; set; } = "default";

    public string? TransportId { get; set; }
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
            "Legacy BrokerDispatch requires a registered execution transport, but the envelope for Run {RunId} (queue {Queue}) could not be routed to any adapter",
            envelope.RunId,
            envelope.Queue);
        throw new InvalidOperationException("No KubeJob execution transport is registered for legacy BrokerDispatch.");
    }
}
