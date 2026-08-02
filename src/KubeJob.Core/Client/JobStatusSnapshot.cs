using System.Text.Json.Serialization;
using KubeJob.Core.Runtime;

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
[method: JsonConstructor]
public sealed record JobStatusSnapshot(
    string JobId,
    JobPhase Phase,
    int AttemptCount,
    DateTimeOffset CreatedAt,
    DateTimeOffset? StartedAt = null,
    DateTimeOffset? CompletedAt = null,
    string? CurrentWorkerId = null,
    string? FailureCode = null,
    string? FailureMessage = null,
    string? ParentRunId = null,
    RunRelationKind RelationKind = RunRelationKind.None)
{
    public JobStatusSnapshot(
        string JobId,
        JobPhase Phase,
        int AttemptCount,
        DateTimeOffset CreatedAt,
        DateTimeOffset? StartedAt,
        DateTimeOffset? CompletedAt,
        string? CurrentWorkerId,
        string? FailureCode,
        string? FailureMessage)
        : this(
            JobId,
            Phase,
            AttemptCount,
            CreatedAt,
            StartedAt,
            CompletedAt,
            CurrentWorkerId,
            FailureCode,
            FailureMessage,
            null,
            RunRelationKind.None)
    {
    }
}
