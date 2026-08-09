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
    Rejected = 7,
    /// <summary>
    /// The handler completion was durably accepted by the control plane and is
    /// awaiting final Run/Attempt state transition. Lease and timeout recovery
    /// must not reclaim an attempt once it reaches this phase.
    /// </summary>
    Completing = 8
}

public enum JobAttemptOutcome
{
    Succeeded = 0,
    RetryableFailure = 1,
    PermanentFailure = 2,
    Canceled = 3,
    TimedOut = 4
}

/// <summary>
/// Durable relationship between a logical run and a terminal-action child.
/// Keeping this relation in the runtime model makes lineage independent of a
/// particular storage adapter or an in-memory metadata convention.
/// </summary>
public enum RunRelationKind
{
    None = 0,
    Continuation = 1,
    Compensation = 2
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
/// Event types written to the Kj2_Outbox table. The transactional outbox is
/// the durable hand-off for PostgresManaged wake-up hints. BrokerNative
/// messages bypass this table and are published directly by their transport.
/// </summary>
public static class OutboxEventTypes
{
    /// <summary>Non-authoritative hint that a logical queue may have claimable work.</summary>
    public const string WorkAvailable = "work-available";

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
    public ExecutionDeliveryProfile DeliveryProfile { get; init; } = ExecutionDeliveryProfile.Pull;
    public string ExecutionLane { get; init; } = "default";
    public string ConsumerGroup { get; init; } = "default";
    public string? TransportId { get; init; }
    public int Priority { get; init; }
    public JobPhase Phase { get; set; } = JobPhase.Pending;
    public DateTimeOffset AvailableAt { get; set; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? StartedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
    public int AttemptCount { get; set; }
    public int MaxAttempts { get; init; } = 1;
    public int TimeoutSeconds { get; init; } = 300;

    /// <summary>
    /// Per-run retry policy override. When non-null, this policy takes precedence
    /// over the global <see cref="JobRuntimeOptions.RetryPolicy"/> for all
    /// retry-delay calculations of this specific logical run.
    /// </summary>
    public RetryPolicy? RetryPolicy { get; init; }

    /// <summary>
    /// Optional continuation that fires when this run reaches a terminal state.
    /// </summary>
    public Continuation? Continuation { get; init; }

    /// <summary>
    /// Optional compensation action for failed runs.
    /// </summary>
    public Compensation? Compensation { get; init; }

    /// <summary>
    /// Extensible metadata bag for run lineage, diagnostics tags,
    /// or custom key-value pairs. Not serialized to the broker.
    /// </summary>
    public IReadOnlyDictionary<string, string?> Metadata { get; init; } =
        new Dictionary<string, string?>();

    public string? IdempotencyKey { get; init; }
    public string? ConcurrencyKey { get; init; }
    public ExecutionOrderingMode OrderingMode { get; init; } = ExecutionOrderingMode.Parallel;
    public string? ParentRunId { get; init; }
    public RunRelationKind RelationKind { get; init; } = RunRelationKind.None;
    /// <summary>Database-assigned submission order used only by KeyOrdered runs.</summary>
    public long OrderingSequence { get; init; }
    public string? ScheduleId { get; init; }
    public DateTimeOffset? ScheduledFor { get; init; }
    public string? CurrentAttemptId { get; set; }
    public string? CurrentWorkerId { get; set; }
    public string? CurrentSessionId { get; set; }
    public bool CancelRequested { get; set; }
    public string? FailureCode { get; set; }
    public string? FailureMessage { get; set; }
    /// <summary>
    /// Monotonically increasing lease generation. A completion or renewal is
    /// valid only while it carries the generation assigned by the latest claim.
    /// </summary>
    public long FenceVersion { get; set; }
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
    /// <summary>
    /// The Run's lease generation at the time this attempt was claimed.
    /// </summary>
    public required long FenceVersion { get; init; }
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
    public string ExecutionLane { get; init; } = "default";
    public string ConsumerGroup { get; init; } = "default";
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
    public ExecutionDeliveryProfile DeliveryProfile { get; init; } = ExecutionDeliveryProfile.Pull;
    public string ExecutionLane { get; init; } = "default";
    public string ConsumerGroup { get; init; } = "default";
    public string? TransportId { get; init; }
    public ExecutionOrderingMode OrderingMode { get; init; } = ExecutionOrderingMode.Parallel;
    /// <summary>
    /// The run's ConcurrencyKey, carried so transport adapters can co-locate
    /// same-key runs on the same physical lane queue.
    /// </summary>
    public string? PartitionKey { get; init; }
    public required string EventType { get; init; }
    public required string PayloadJson { get; init; }
    public OutboxDeliveryState State { get; set; } = OutboxDeliveryState.Pending;
    public int PublishAttempts { get; set; }
    public DateTimeOffset AvailableAt { get; set; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? PublishedAt { get; set; }
    public string? ClaimToken { get; set; }
    public string? LastError { get; set; }
}
