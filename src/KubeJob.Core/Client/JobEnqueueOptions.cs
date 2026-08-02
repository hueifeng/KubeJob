using KubeJob.Core.Queues;

namespace KubeJob.Core.Client;

/// <summary>
/// Optional submission policy for a logical job run.
/// </summary>
public sealed class JobEnqueueOptions
{
    /// <summary>
    /// Optional business resource pool. When omitted, the typed client's
    /// <see cref="Jobs.JobKey{TPayload}"/> becomes the logical queue.
    /// </summary>
    public string? Queue { get; init; }

    public int Priority { get; init; }

    public DateTimeOffset? NotBefore { get; init; }

    public string? IdempotencyKey { get; init; }

    public string? ConcurrencyKey { get; init; }

    /// <summary>
    /// Maximum number of physical attempts for this logical job, including the first attempt.
    /// </summary>
    public int MaxAttempts { get; init; } = 1;

    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Per-run retry policy. When set, it overrides the global
    /// <see cref="Runtime.JobRuntimeOptions.RetryPolicy"/> for this specific run.
    /// </summary>
    public Runtime.RetryPolicy? RetryPolicy { get; init; }

    /// <summary>
    /// Optional continuation that fires when this run reaches a terminal state.
    /// </summary>
    public Runtime.Continuation? Continuation { get; init; }

    /// <summary>
    /// Optional compensation action for failed runs.
    /// </summary>
    public Runtime.Compensation? Compensation { get; init; }

    public void Validate()
    {
        if (Queue is not null && string.IsNullOrWhiteSpace(Queue))
        {
            throw new ArgumentException("Queue cannot be empty when explicitly specified.", nameof(Queue));
        }

        if (MaxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxAttempts), "MaxAttempts must be at least one.");
        }

        if (Timeout <= TimeSpan.Zero || Timeout > TimeSpan.FromDays(1))
        {
            throw new ArgumentOutOfRangeException(nameof(Timeout), "Timeout must be between zero and one day.");
        }

        RetryPolicy?.Validate();
    }

    public string ResolveQueue(string jobKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobKey);
        Validate();
        return LogicalQueueName.Normalize(Queue?.Trim() ?? jobKey, nameof(Queue));
    }
}
