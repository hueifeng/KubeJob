using KubeJob.Core.Queues;

namespace KubeJob.Worker.Options;

/// <summary>
/// Configuration options for a KubeJob worker process.
/// </summary>
public sealed class KubeJobWorkerOptions
{
    private const int MaximumMetadataItems = 256;

    /// <summary>
    /// HTTP endpoint of the KubeJob control plane. Unified hosts replace the
    /// transport and do not make localhost HTTP calls.
    /// </summary>
    public string ServerEndpoint { get; set; } = "http://localhost:5000";

    /// <summary>
    /// Maximum number of handlers executing concurrently in this process.
    /// The default (64) is chosen so a single worker can keep the storage
    /// backend's commit throughput saturated without the operator having to
    /// tune it. Raise this when the storage backend can sustain more parallel
    /// transactions (e.g. an SSD-backed Postgres with a fast WAL).
    /// </summary>
    public int MaxConcurrentJobs { get; set; } = 64;

    /// <summary>
    /// Stable worker identity. Each process start receives a unique session
    /// identity and a monotonically increasing session epoch.
    /// </summary>
    public string WorkerId { get; set; } = Environment.MachineName;

    /// <summary>
    /// Consumer group owned by this worker process. A worker belongs to exactly
    /// one group.
    /// </summary>
    public string ConsumerGroup { get; set; } = "default";
    public string ExecutionLane { get; set; } = "default";

    public Dictionary<string, string> Labels { get; set; } = new();

    /// <summary>
    /// Queues this worker is allowed to claim from. A worker must declare its
    /// business queues explicitly; there is no implicit catch-all queue.
    /// </summary>
    public List<string> Queues { get; set; } = new();

    public string BuildId { get; set; } = "unknown";
    public int ClaimBatchSize { get; set; } = 32;
    public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan LeaseRenewalInterval { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Grace period for in-flight attempts during shutdown and the fence
    /// deadline. On shutdown the worker waits up to this long for attempts to
    /// complete; a handler that ignores cancellation beyond the deadline does
    /// not block process exit (the lease reaper reclaims its attempt). When
    /// the session is fenced, the worker fails its hosted service after at
    /// most this period even if a handler is still running.
    /// </summary>
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum failure detail persisted for one Attempt. Logs retain the original
    /// exception; durable state is bounded to protect storage and Dashboard responses.
    /// </summary>
    public int MaximumFailureMessageLength { get; set; } = 32 * 1024;

    /// <summary>
    /// Ordered list of execution middleware types. Middleware is invoked in
    /// registration order, wrapping the handler invocation.
    /// </summary>
    public IList<Type> ExecutionMiddleware { get; init; } = [];

    public void Validate()
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
        if (Queues.Count == 0)
        {
            throw new InvalidOperationException("At least one non-empty queue is required.");
        }

        if (Queues.Count > MaximumMetadataItems)
        {
            throw new InvalidOperationException($"A worker cannot register more than {MaximumMetadataItems} queues.");
        }

        if (Queues.Any(queue => queue.Length > 100))
        {
            throw new InvalidOperationException("Queue names cannot exceed 100 characters.");
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
