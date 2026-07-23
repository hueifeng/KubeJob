namespace KubeJob.Worker.Options;

/// <summary>
/// Configuration options for a KubeJob worker process.
/// </summary>
public sealed class KubeJobWorkerOptions
{
    /// <summary>
    /// HTTP endpoint of the KubeJob control plane. Unified hosts replace the
    /// transport and do not make localhost HTTP calls.
    /// </summary>
    public string ServerEndpoint { get; set; } = "http://localhost:5000";

    /// <summary>
    /// Maximum number of handlers executing concurrently in this process.
    /// </summary>
    public int MaxConcurrentJobs { get; set; } = 10;

    /// <summary>
    /// Stable worker identity. Each process start receives a unique session
    /// identity and a monotonically increasing session epoch.
    /// </summary>
    public string WorkerId { get; set; } = Environment.MachineName;

    public Dictionary<string, string> Labels { get; set; } = new();

    /// <summary>
    /// Queues this worker is allowed to claim from.
    /// </summary>
    public List<string> Queues { get; set; } = new() { "default" };

    public string BuildId { get; set; } = "unknown";
    public int ClaimBatchSize { get; set; } = 16;
    public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan LeaseRenewalInterval { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public void ValidateV2() => Validate();

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

        if (string.IsNullOrWhiteSpace(WorkerId))
        {
            throw new InvalidOperationException("WorkerId is required.");
        }

        if (MaxConcurrentJobs < 1)
        {
            throw new InvalidOperationException("MaxConcurrentJobs must be positive.");
        }

        if (ClaimBatchSize is < 1 or > 1024)
        {
            throw new InvalidOperationException("ClaimBatchSize must be between 1 and 1024.");
        }

        if (Queues.Count == 0 || Queues.Any(string.IsNullOrWhiteSpace))
        {
            throw new InvalidOperationException("At least one non-empty queue is required.");
        }

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
    }
}
