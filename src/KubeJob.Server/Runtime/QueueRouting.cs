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

    internal void Validate()
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
