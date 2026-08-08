using KubeJob.Core.Queues;

namespace KubeJob.Core.Client;

/// <summary>
/// Optional submission policy for a KubeJob job. Some policies require
/// PostgresManaged durable state and are rejected by BrokerNative until the
/// selected transport/runtime explicitly implements equivalent semantics.
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

    /// <summary>
    /// Durable submission deduplication key for PostgresManaged jobs.
    /// BrokerNative currently rejects this option because no Inbox/deduplication
    /// store is implemented; carrying the value in a broker header alone would
    /// not provide duplicate suppression.
    /// </summary>
    public string? IdempotencyKey { get; init; }

    public string? ConcurrencyKey { get; init; }

    /// <summary>
    /// Maximum number of physical attempts for this job, including the first attempt.
    /// </summary>
    public int MaxAttempts { get; init; } = 1;

    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Per-run retry policy for PostgresManaged. BrokerNative currently uses
    /// transport-owned retry timing and rejects this override.
    /// </summary>
    public Runtime.RetryPolicy? RetryPolicy { get; init; }

    /// <summary>
    /// Optional PostgresManaged continuation that fires when this run reaches a terminal state.
    /// </summary>
    public Runtime.Continuation? Continuation { get; init; }

    /// <summary>
    /// Optional PostgresManaged compensation action for failed runs.
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
