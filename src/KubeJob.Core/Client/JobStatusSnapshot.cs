namespace KubeJob.Core.Client;

public enum JobPhase
{
    Pending = 0,
    Running = 1,
    Succeeded = 2,
    Failed = 3,
    Canceled = 4,
    Dead = 5
}

/// <summary>
/// Latest-known status of a logical job run.
/// </summary>
public sealed record JobStatusSnapshot(
    string JobId,
    JobPhase Phase,
    int AttemptCount,
    DateTimeOffset CreatedAt,
    DateTimeOffset? StartedAt = null,
    DateTimeOffset? CompletedAt = null,
    string? CurrentWorkerId = null,
    string? FailureCode = null,
    string? FailureMessage = null);
