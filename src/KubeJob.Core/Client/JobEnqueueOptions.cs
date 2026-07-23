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

    /// <summary>
    /// Maximum number of physical attempts for this logical job, including the first attempt.
    /// </summary>
    public int MaxAttempts { get; init; } = 1;

    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(5);

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Queue);

        if (MaxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxAttempts), "MaxAttempts must be at least one.");
        }

        if (Timeout <= TimeSpan.Zero || Timeout > TimeSpan.FromDays(1))
        {
            throw new ArgumentOutOfRangeException(nameof(Timeout), "Timeout must be between zero and one day.");
        }
    }
}
