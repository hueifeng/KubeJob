using KubeJob.Core.Queues;

namespace KubeJob.Worker.Options;

/// <summary>
/// Configuration options shared by Managed workers and BrokerNative workers.
/// Job Queue membership is required by Managed/Job consumers but event-only
/// BrokerNative workers may legitimately have no Job queues.
/// </summary>
public sealed class KubeJobWorkerOptions
{
    private const int MaximumMetadataItems = 256;

    public string ServerEndpoint { get; set; } = "http://localhost:5000";

    public int MaxConcurrentJobs { get; set; } = 64;

    public string WorkerId { get; set; } = Environment.MachineName;

    /// <summary>
    /// Managed worker-group identity. Event subscriptions have their own
    /// explicit subscription names.
    /// </summary>
    public string ConsumerGroup { get; set; } = "default";

    /// <summary>
    /// Managed execution lane used for worker eligibility and ordering.
    /// </summary>
    public string ExecutionLane { get; set; } = "default";

    public Dictionary<string, string> Labels { get; set; } = new();

    /// <summary>
    /// Logical Job Queues consumed by this process. Event-only BrokerNative
    /// workers may leave this empty.
    /// </summary>
    public List<string> Queues { get; set; } = new();

    public string BuildId { get; set; } = "unknown";
    public int ClaimBatchSize { get; set; } = 32;
    public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan LeaseRenewalInterval { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public int MaximumFailureMessageLength { get; set; } = 32 * 1024;
    public IList<Type> ExecutionMiddleware { get; init; } = [];

    /// <summary>
    /// Validates a Managed or BrokerNative Job worker and therefore requires at
    /// least one logical Job Queue. The public API remains parameterless so
    /// existing method-group callers keep compiling.
    /// </summary>
    public void Validate() => ValidateCore(requireJobQueues: true);

    /// <summary>
    /// Validates an Event-only BrokerNative worker without requiring a fake Job
    /// Queue. Event subscriptions themselves define its delivery streams.
    /// </summary>
    public void ValidateEventWorker() => ValidateCore(requireJobQueues: false);

    private void ValidateCore(bool requireJobQueues)
    {
        if (!Uri.TryCreate(ServerEndpoint, UriKind.Absolute, out var endpoint)
            || endpoint.Scheme is not ("http" or "https"))
        {
            throw new InvalidOperationException("ServerEndpoint must be an absolute HTTP or HTTPS URI.");
        }

        ServerEndpoint = endpoint.AbsoluteUri.EndsWith("/", StringComparison.Ordinal)
            ? endpoint.AbsoluteUri
            : endpoint.AbsoluteUri + "/";

        WorkerId = WorkerId?.Trim() ?? string.Empty;
        if (WorkerId.Length is < 1 or > 200)
        {
            throw new InvalidOperationException("WorkerId must contain between 1 and 200 characters.");
        }

        ConsumerGroup = ConsumerGroup?.Trim() ?? string.Empty;
        if (ConsumerGroup.Length is < 1 or > 200)
        {
            throw new InvalidOperationException("ConsumerGroup must contain between 1 and 200 characters.");
        }

        ExecutionLane = ExecutionLane?.Trim() ?? string.Empty;
        if (ExecutionLane.Length is < 1 or > 200)
        {
            throw new InvalidOperationException("ExecutionLane must contain between 1 and 200 characters.");
        }

        BuildId = string.IsNullOrWhiteSpace(BuildId) ? "unknown" : BuildId.Trim();
        if (BuildId.Length > 300)
        {
            throw new InvalidOperationException("BuildId cannot exceed 300 characters.");
        }

        if (MaxConcurrentJobs is < 1 or > 10_000)
        {
            throw new InvalidOperationException("MaxConcurrentJobs must be between 1 and 10000.");
        }

        if (ClaimBatchSize is < 1 or > 1024)
        {
            throw new InvalidOperationException("ClaimBatchSize must be between 1 and 1024.");
        }

        if (Queues is null)
        {
            throw new InvalidOperationException("Queues cannot be null.");
        }

        Queues = Queues
            .Select(queue => LogicalQueueName.Normalize(queue ?? string.Empty))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (requireJobQueues && Queues.Count == 0)
        {
            throw new InvalidOperationException("At least one non-empty Job queue is required.");
        }

        if (Queues.Count > MaximumMetadataItems)
        {
            throw new InvalidOperationException($"A worker cannot register more than {MaximumMetadataItems} queues.");
        }

        if (Labels is null)
        {
            throw new InvalidOperationException("Labels cannot be null.");
        }

        if (Labels.Count > MaximumMetadataItems)
        {
            throw new InvalidOperationException($"A worker cannot register more than {MaximumMetadataItems} labels.");
        }

        var normalizedLabels = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var label in Labels)
        {
            var key = label.Key?.Trim() ?? string.Empty;
            var value = label.Value ?? string.Empty;
            if (key.Length is < 1 or > 200 || value.Length > 1000)
            {
                throw new InvalidOperationException(
                    "Label keys must contain 1-200 characters and values cannot exceed 1000 characters.");
            }

            if (!normalizedLabels.TryAdd(key, value))
            {
                throw new InvalidOperationException(
                    $"Worker labels contain duplicate key '{key}' after normalization.");
            }
        }

        Labels = normalizedLabels;

        if (EmptyPollDelay <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("EmptyPollDelay must be positive.");
        }

        if (LeaseRenewalInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("LeaseRenewalInterval must be positive.");
        }

        if (HeartbeatInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("HeartbeatInterval must be positive.");
        }

        if (DrainTimeout < TimeSpan.Zero)
        {
            throw new InvalidOperationException("DrainTimeout cannot be negative.");
        }

        if (MaximumFailureMessageLength is < 1024 or > 1024 * 1024)
        {
            throw new InvalidOperationException(
                "MaximumFailureMessageLength must be between 1024 and 1048576 characters.");
        }
    }
}
