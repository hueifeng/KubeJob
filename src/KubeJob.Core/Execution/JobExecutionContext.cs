namespace KubeJob.Core.Execution;

/// <summary>
/// Read-only information about the current logical run and physical attempt.
/// Business handlers receive this context but resolve dependencies through constructor injection.
/// </summary>
public sealed class JobExecutionContext
{
    public required string RunId { get; init; }

    public required string AttemptId { get; init; }

    public required int AttemptNumber { get; init; }

    public string? BatchId { get; init; }

    public int? ShardIndex { get; init; }

    public int? ShardCount { get; init; }

    public required WorkerExecutionInfo Worker { get; init; }

    public DateTimeOffset StartedAt { get; init; }
}
