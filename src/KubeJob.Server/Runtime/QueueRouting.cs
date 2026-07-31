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

public interface IExecutionGroupResolver
{
    string Resolve(string logicalQueue);
}

public sealed class DefaultExecutionGroupResolver : IExecutionGroupResolver
{
    private readonly IOptions<QueueDeliveryOptions> _options;

    public DefaultExecutionGroupResolver(IOptions<QueueDeliveryOptions> options)
    {
        _options = options;
    }

    public string Resolve(string logicalQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        var options = _options.Value;
        if (options.QueueGroups.TryGetValue(logicalQueue, out var configured)
            && !string.IsNullOrWhiteSpace(configured))
        {
            return configured;
        }

        return string.IsNullOrWhiteSpace(options.DefaultExecutionGroup)
            ? "default"
            : options.DefaultExecutionGroup!;
    }
}

/// <summary>
/// Deployment-level routing policy. It intentionally has no relationship to
/// EnqueueJobRequest, so a business caller cannot choose a physical target for
/// an individual Run.
/// </summary>
public sealed class QueueDeliveryOptions
{
    public ExecutionDeliveryProfile DefaultProfile { get; set; } = ExecutionDeliveryProfile.BrokerDispatch;
    public string DefaultExecutionLane { get; set; } = "default";
    public string? DefaultTransportId { get; set; } = "rabbitmq";
    public Dictionary<string, ExecutionDeliveryProfile> QueueProfiles { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, string> QueueGroups { get; } = new(StringComparer.Ordinal);
    public string? DefaultExecutionGroup { get; set; }

    public void Validate()
    {
        if (!Enum.IsDefined(DefaultProfile))
        {
            throw new InvalidOperationException($"Unsupported default execution delivery profile '{DefaultProfile}'.");
        }

        if (string.IsNullOrWhiteSpace(DefaultExecutionLane))
        {
            throw new InvalidOperationException("Default execution lane is required.");
        }

        if (DefaultProfile == ExecutionDeliveryProfile.BrokerDispatch
            && string.IsNullOrWhiteSpace(DefaultTransportId))
        {
            throw new InvalidOperationException("Broker dispatch requires DefaultTransportId.");
        }

        if (QueueProfiles.Any(entry => string.IsNullOrWhiteSpace(entry.Key) || entry.Key.Length > 100))
        {
            throw new InvalidOperationException("Queue delivery policy contains an invalid logical queue.");
        }

        if (QueueProfiles.Values.Any(profile => !Enum.IsDefined(profile)))
        {
            throw new InvalidOperationException("Queue delivery policy contains an unsupported execution profile.");
        }

        if (QueueGroups.Any(entry => string.IsNullOrWhiteSpace(entry.Key)))
        {
            throw new InvalidOperationException("Queue execution group policy contains an empty logical queue identifier.");
        }

        if (QueueGroups.Any(entry => string.IsNullOrWhiteSpace(entry.Value)))
        {
            throw new InvalidOperationException("Queue execution group policy contains an empty group identifier.");
        }

        if (DefaultExecutionGroup is { Length: > 200 })
        {
            throw new InvalidOperationException("DefaultExecutionGroup must not exceed 200 characters.");
        }
    }
}

public sealed class ConfigurationQueueRouter : IQueueRouter
{
    private readonly IOptions<QueueDeliveryOptions> _options;
    private readonly IExecutionGroupResolver _groups;

    public ConfigurationQueueRouter(
        IOptions<QueueDeliveryOptions> options,
        IExecutionGroupResolver groups)
    {
        _options = options;
        _groups = groups;
    }

    public QueueRoute Resolve(string logicalQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        var options = _options.Value;
        options.Validate();
        var profile = options.QueueProfiles.TryGetValue(logicalQueue, out var configured)
            ? configured
            : options.DefaultProfile;
        var target = new DeliveryTarget(
            profile,
            _groups.Resolve(logicalQueue),
            profile == ExecutionDeliveryProfile.BrokerDispatch ? options.DefaultTransportId : null);
        target.Validate();
        return new QueueRoute(logicalQueue, target);
    }
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
            "Execution lane {ExecutionLane} requires transport {TransportId}, but no matching adapter is registered",
            envelope.ExecutionLane,
            TransportId);
        throw new InvalidOperationException("No KubeJob execution transport is registered for broker dispatch.");
    }
}
