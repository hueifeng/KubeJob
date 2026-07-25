using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public sealed record DashboardPage<T>(
    IReadOnlyList<T> Items,
    int TotalCount,
    int Page,
    int PageSize)
{
    public int TotalPages => TotalCount == 0
        ? 0
        : (int)Math.Ceiling(TotalCount / (double)PageSize);
}

public sealed record DashboardRunQuery(
    int Page = 1,
    int PageSize = 25,
    JobPhase? Phase = null,
    string? Queue = null,
    string? JobKey = null)
{
    public DashboardRunQuery Normalize()
    {
        var page = Math.Max(1, Page);
        var pageSize = Math.Clamp(PageSize, 10, 100);
        return this with
        {
            Page = page,
            PageSize = pageSize,
            Queue = string.IsNullOrWhiteSpace(Queue) ? null : Queue.Trim(),
            JobKey = string.IsNullOrWhiteSpace(JobKey) ? null : JobKey.Trim()
        };
    }
}

/// <summary>
/// Payload-free projection used by dashboard list pages. Large serialized payloads
/// are loaded only for an explicitly opened Run detail page.
/// </summary>
public sealed class DashboardRunSummary
{
    public required string Id { get; init; }
    public required string JobKey { get; init; }
    public string Queue { get; init; } = "default";
    public int Priority { get; init; }
    public JobPhase Phase { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? StartedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public int AttemptCount { get; init; }
    public int MaxAttempts { get; init; }
    public string? ScheduleId { get; init; }
    public string? CurrentWorkerId { get; init; }
    public string? CurrentSessionId { get; init; }
    public bool CancelRequested { get; init; }
    public string? FailureCode { get; init; }
    public string? FailureMessage { get; init; }
}

/// <summary>
/// Sanitized Run projection for the operations dashboard. PayloadJson is populated
/// only when the host explicitly enables payload rendering.
/// </summary>
public sealed class DashboardRunDetails
{
    public required string Id { get; init; }
    public required string JobKey { get; init; }
    public string? PayloadJson { get; init; }
    public string Queue { get; init; } = "default";
    public int Priority { get; init; }
    public JobPhase Phase { get; init; }
    public DateTimeOffset AvailableAt { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? StartedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public int AttemptCount { get; init; }
    public int MaxAttempts { get; init; }
    public int TimeoutSeconds { get; init; }
    public string? IdempotencyKey { get; init; }
    public string? ConcurrencyKey { get; init; }
    public string? ScheduleId { get; init; }
    public DateTimeOffset? ScheduledFor { get; init; }
    public string? CurrentWorkerId { get; init; }
    public bool CancelRequested { get; init; }
    public string? FailureCode { get; init; }
    public string? FailureMessage { get; init; }
}

/// <summary>
/// Credential-free Attempt projection for the operations dashboard. LeaseToken is
/// deliberately absent so the presentation layer cannot render fencing credentials.
/// </summary>
public sealed class DashboardAttemptSummary
{
    public required string Id { get; init; }
    public required int AttemptNumber { get; init; }
    public required string WorkerId { get; init; }
    public required string SessionId { get; init; }
    public required long SessionEpoch { get; init; }
    public JobAttemptPhase Phase { get; init; }
    public DateTimeOffset ClaimedAt { get; init; }
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset LeaseExpiresAt { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public string? FailureCode { get; init; }
    public string? FailureMessage { get; init; }
}

public sealed record DashboardQueueSummary(
    string Queue,
    int PendingRuns,
    int RunningRuns,
    DateTimeOffset? OldestReadyAt)
{
    public int ActiveRuns => PendingRuns + RunningRuns;
}

public sealed record DashboardActivitySummary(
    int SucceededRuns,
    int FailedRuns,
    int CanceledRuns,
    int DeadRuns)
{
    public int CompletedRuns => SucceededRuns + FailedRuns + CanceledRuns + DeadRuns;
    public int UnsuccessfulRuns => FailedRuns + DeadRuns;
}

public sealed record DashboardJobTypeSummary(
    string JobKey,
    int WorkerCount,
    int ReadyWorkerCount,
    IReadOnlyList<string> WorkerIds,
    IReadOnlyList<string> Queues,
    int RecentRunCount,
    DateTimeOffset? LastRunAt,
    JobPhase? LastPhase,
    int ScheduleCount)
{
    public bool IsAvailable => ReadyWorkerCount > 0;
}

public sealed record DashboardJobTypesViewModel(
    IReadOnlyList<DashboardJobTypeSummary> JobTypes,
    int RecentRunLimit,
    DateTimeOffset ObservedAt);

public sealed record DashboardOverview(
    DateTimeOffset ObservedAt,
    int PendingRuns,
    int RunningRuns,
    int SucceededRuns,
    int FailedRuns,
    int CanceledRuns,
    int DeadRuns,
    int ReadyWorkers,
    int DrainingWorkers,
    int TotalWorkerCapacity,
    int AvailableWorkerSlots,
    int EnabledSchedules,
    int DisabledSchedules,
    int PendingOutboxMessages,
    DashboardActivitySummary LastHour,
    IReadOnlyList<DashboardQueueSummary> Queues,
    IReadOnlyList<DashboardRunSummary> RecentRuns);

public interface IJobRuntimeDashboardStore
{
    ValueTask<DashboardOverview> GetOverviewAsync(
        int recentRunCount,
        CancellationToken cancellationToken);

    ValueTask<DashboardPage<DashboardRunSummary>> GetRunsAsync(
        DashboardRunQuery query,
        CancellationToken cancellationToken);

    ValueTask<DashboardRunDetails?> GetRunDetailsAsync(
        string runId,
        bool includePayload,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<DashboardAttemptSummary>> GetAttemptSummariesAsync(
        string runId,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<WorkerSessionRecord>> GetWorkerSessionsAsync(
        int limit,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<JobScheduleRecord>> GetSchedulesAsync(
        int limit,
        CancellationToken cancellationToken);
}
