namespace KubeJob.Core.Client;

/// <summary>
/// Optional submission policy for a logical job run.
/// </summary>
public sealed class JobEnqueueOptions
{
    public string Queue { get; init; } = "default";

    public int Priority { get; init; }

    public DateTimeOffset? NotBefore { get; init; }

    public string? IdempotencyKey { get; init; }

    public string? ConcurrencyKey { get; init; }
}
