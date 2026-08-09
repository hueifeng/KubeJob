using KubeJob.Core.Scheduling;

namespace KubeJob.Core.Runtime;

public sealed class JobScheduleRecord
{
    public required string Id { get; init; }
    public required string JobKey { get; init; }
    public required string PayloadJson { get; init; }
    public required string CronExpression { get; init; }
    public string TimeZoneId { get; init; } = "UTC";
    public required string Queue { get; init; }

    // Managed metadata retained for storage/schema compatibility. BrokerNative
    // schedule dispatch resolves QueueRuntimeMode at fire time.
    public string ExecutionLane { get; init; } = "default";
    public ExecutionDeliveryProfile DeliveryProfile { get; init; } = ExecutionDeliveryProfile.Pull;
    public string ConsumerGroup { get; init; } = "default";
    public string? TransportId { get; init; }
    public ExecutionOrderingMode OrderingMode { get; init; } = ExecutionOrderingMode.Parallel;

    public int Priority { get; init; }
    public MisfirePolicy MisfirePolicy { get; init; }
    public ScheduleConcurrencyPolicy ConcurrencyPolicy { get; init; }
    public int MaxAttempts { get; init; } = 1;
    public int TimeoutSeconds { get; init; } = 300;
    public string? ConcurrencyKey { get; init; }
    public RetryPolicy? RetryPolicy { get; init; }
    public bool Enabled { get; set; } = true;
    public DateTimeOffset NextFireAt { get; set; }
    public DateTimeOffset? LastFireAt { get; set; }
    public string? ClaimToken { get; set; }
    public DateTimeOffset? ClaimUntil { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
    public long Version { get; set; }
}

public sealed record ClaimedSchedule(
    JobScheduleRecord Schedule,
    string ClaimToken,
    long ExpectedVersion);

public sealed record CommitScheduleFireCommand(
    string ScheduleId,
    string ClaimToken,
    long ExpectedVersion,
    DateTimeOffset ScheduledFor,
    DateTimeOffset NextFireAt,
    bool CreateRun,
    string RunId,
    string IdempotencyKey);
