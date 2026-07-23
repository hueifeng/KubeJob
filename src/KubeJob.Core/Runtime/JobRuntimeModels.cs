using KubeJob.Core.Client;

namespace KubeJob.Core.Runtime;

public enum JobAttemptPhase
{
    Running = 0,
    Succeeded = 1,
    RetryableFailure = 2,
    PermanentFailure = 3,
    Canceled = 4,
    TimedOut = 5,
    LeaseLost = 6,
    Rejected = 7
}

public enum JobAttemptOutcome
{
    Succeeded = 0,
    RetryableFailure = 1,
    PermanentFailure = 2,
    Canceled = 3,
    TimedOut = 4
}

public enum WorkerSessionState
{
    Ready = 0,
    Draining = 1,
    Closed = 2,
    Stale = 3
}

public enum OutboxDeliveryState
{
    Pending = 0,
    Publishing = 1,
    Published = 2,
    Failed = 3,
    Abandoned = 4
}

/// <summary>
/// Durable logical job. Retries create attempts, not additional logical runs.
/// </summary>
public sealed class JobRunRecord
{
    public required string Id { get; init; }
    public required string JobKey { get; init; }
    public required string PayloadJson { get; init; }
    public string Queue { get; init; } = "default";
    public int Priority { get; init; }
    public JobPhase Phase { get; set; } = JobPhase.Pending;
    public DateTimeOffset AvailableAt { get; set; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? StartedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
    public int AttemptCount { get; set; }
    public int MaxAttempts { get; init; } = 1;
    public int TimeoutSeconds { get; init; } = 300;
    public string? IdempotencyKey { get; init; }
    public string? ConcurrencyKey { get; init; }
    public string? CurrentAttemptId { get; set; }
    public string? CurrentWorkerId { get; set; }
    public string? CurrentSessionId { get; set; }
    public bool CancelRequested { get; set; }
    public string? FailureCode { get; set; }
    public string? FailureMessage { get; set; }
    public long Version { get; set; }
}

/// <summary>
/// One physical execution of a logical job.
/// </summary>
public sealed class JobAttemptRecord
{
    public required string Id { get; init; }
    public required string RunId { get; init; }
    public required int AttemptNumber { get; init; }
    public required string WorkerId { get; init; }
    public required string SessionId { get; init; }
    public required long SessionEpoch { get; init; }
    public required string LeaseToken { get; init; }
    public JobAttemptPhase Phase { get; set; } = JobAttemptPhase.Running;
    public DateTimeOffset ClaimedAt { get; init; }
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset LeaseExpiresAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
    public string? FailureCode { get; set; }
    public string? FailureMessage { get; set; }
}

public sealed class WorkerSessionRecord
{
    public required string WorkerId { get; init; }
    public required string SessionId { get; init; }
    public required long Epoch { get; init; }
    public string? BuildId { get; init; }
    public string? HostName { get; init; }
    public WorkerSessionState State { get; set; } = WorkerSessionState.Ready;
    public int MaxConcurrency { get; init; }
    public int AvailableSlots { get; set; }
    public IReadOnlyList<string> Queues { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> Capabilities { get; init; } = Array.Empty<string>();
    public IReadOnlyDictionary<string, string> Labels { get; init; } = new Dictionary<string, string>();
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset LastHeartbeatAt { get; set; }
}

public sealed class OutboxMessageRecord
{
    public required string Id { get; init; }
    public required string Queue { get; init; }
    public required string EventType { get; init; }
    public required string PayloadJson { get; init; }
    public OutboxDeliveryState State { get; set; } = OutboxDeliveryState.Pending;
    public int PublishAttempts { get; set; }
    public DateTimeOffset AvailableAt { get; set; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? PublishedAt { get; set; }
    public string? LastError { get; set; }
}
