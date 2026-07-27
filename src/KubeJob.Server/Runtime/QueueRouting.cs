using KubeJob.Core.Runtime;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Runtime;

public sealed record QueueRoute(
    string Queue,
    ExecutionDeliveryProfile Profile);

public interface IQueueRouter
{
    QueueRoute Resolve(string logicalQueue);
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

        if (!string.IsNullOrWhiteSpace(options.DefaultExecutionGroup))
        {
            return options.DefaultExecutionGroup!;
        }

        return "default";
    }
}

/// <summary>
/// Deployment-level routing policy. It intentionally has no relationship to
/// EnqueueJobRequest, so a business caller cannot choose a physical profile
/// for an individual Run.
/// </summary>
public sealed class QueueDeliveryOptions
{
    public ExecutionDeliveryProfile DefaultProfile { get; set; } = ExecutionDeliveryProfile.Pull;

    public Dictionary<string, ExecutionDeliveryProfile> QueueProfiles { get; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Optional per-queue execution group mapping. When a logical queue is
    /// routed to BrokerDispatch, this group's identifier is embedded in the
    /// cancel outbox row so transport adapters (RabbitMQ, NATS, etc.) can
    /// fan out a per-group cancel signal. Queues not present here fall back
    /// to <see cref="DefaultExecutionGroup"/>, which itself defaults to
    /// <c>"default"</c>.
    /// </summary>
    public Dictionary<string, string> QueueGroups { get; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Default execution group used when a logical queue is not listed in
    /// <see cref="QueueGroups"/>. Leaving this unset preserves the historical
    /// single-group behavior (group = <c>"default"</c>).
    /// </summary>
    public string? DefaultExecutionGroup { get; set; }

    public void Validate()
    {
        if (!Enum.IsDefined(DefaultProfile))
        {
            throw new InvalidOperationException(
                $"Unsupported default execution delivery profile '{DefaultProfile}'.");
        }

        if (QueueProfiles.Any(entry => string.IsNullOrWhiteSpace(entry.Key)))
        {
            throw new InvalidOperationException(
                "Queue delivery policy cannot contain an empty logical queue.");
        }

        if (QueueProfiles.Keys.Any(queue => queue.Length > 100))
        {
            throw new InvalidOperationException(
                "Logical queue names cannot exceed 100 characters.");
        }

        if (QueueProfiles.Values.Any(profile => !Enum.IsDefined(profile)))
        {
            throw new InvalidOperationException(
                "Queue delivery policy contains an unsupported execution profile.");
        }

        if (QueueGroups.Any(entry => string.IsNullOrWhiteSpace(entry.Key)))
        {
            throw new InvalidOperationException(
                "Queue execution group policy cannot contain an empty logical queue.");
        }

        if (QueueGroups.Any(entry => string.IsNullOrWhiteSpace(entry.Value)))
        {
            throw new InvalidOperationException(
                "Queue execution group policy cannot contain an empty group identifier.");
        }

        if (!string.IsNullOrWhiteSpace(DefaultExecutionGroup)
            && DefaultExecutionGroup!.Length > 200)
        {
            throw new InvalidOperationException(
                "DefaultExecutionGroup cannot exceed 200 characters.");
        }
    }
}

public sealed class ConfigurationQueueRouter : IQueueRouter
{
    private readonly IOptions<QueueDeliveryOptions> _options;

    public ConfigurationQueueRouter(IOptions<QueueDeliveryOptions> options)
    {
        _options = options;
    }

    public QueueRoute Resolve(string logicalQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        var options = _options.Value;
        options.Validate();
        var profile = options.QueueProfiles.TryGetValue(logicalQueue, out var configured)
            ? configured
            : options.DefaultProfile;
        return new QueueRoute(logicalQueue, profile);
    }
}

public sealed class UnconfiguredExecutionDispatcher : IExecutionDispatcher
{
    private readonly ILogger<UnconfiguredExecutionDispatcher> _logger;

    public UnconfiguredExecutionDispatcher(
        ILogger<UnconfiguredExecutionDispatcher> logger)
    {
        _logger = logger;
    }

    public ValueTask DispatchAsync(
        ExecutionEnvelope envelope,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(envelope);
        _logger.LogError(
            "Queue {Queue} is configured for broker dispatch, but no execution dispatcher adapter is registered",
            envelope.Queue);
        throw new InvalidOperationException(
            "No KubeJob execution dispatcher adapter is registered for broker dispatch.");
    }
}
