using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public sealed record SubmitJobCommand(
    string JobKey,
    string PayloadJson,
    string Queue,
    int Priority,
    DateTimeOffset AvailableAt,
    string? IdempotencyKey,
    string? ConcurrencyKey,
    int MaxAttempts,
    int TimeoutSeconds,
    string? ScheduleId = null,
    DateTimeOffset? ScheduledFor = null);

public sealed record SubmitJobResult(JobRunRecord Run, bool Existing);

public interface IJobSubmissionStore
{
    ValueTask<SubmitJobResult> SubmitAsync(
        SubmitJobCommand command,
        CancellationToken cancellationToken);

    ValueTask<bool> RequestCancelAsync(
        string runId,
        string? reason,
        CancellationToken cancellationToken);
}

public interface IWorkerSessionStore
{
    ValueTask<WorkerSessionRecord> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken);

    ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken);

    ValueTask<bool> CloseAsync(
        string workerId,
        string sessionId,
        long sessionEpoch,
        CancellationToken cancellationToken);
}

public interface IJobClaimStore
{
    ValueTask<IReadOnlyList<ClaimedJob>> ClaimAsync(
        ClaimJobsRequest request,
        TimeSpan leaseDuration,
        int maxBatchSize,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<LeaseRenewalResult>> RenewLeasesAsync(
        RenewLeasesRequest request,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken);
}

public interface IJobCompletionStore
{
    ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        TimeSpan retryDelay,
        CancellationToken cancellationToken);

    ValueTask<int> RequeueExpiredLeasesAsync(
        DateTimeOffset now,
        TimeSpan retryDelay,
        int batchSize,
        CancellationToken cancellationToken);
}

public interface IJobQueryStore
{
    ValueTask<JobRunRecord?> GetRunAsync(
        string runId,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<JobAttemptRecord>> GetAttemptsAsync(
        string runId,
        CancellationToken cancellationToken);
}

public interface IJobScheduleStore
{
    /// <summary>
    /// Creates a schedule only when its ID is not already present. Returns null
    /// when another writer already owns the ID.
    /// </summary>
    ValueTask<JobScheduleRecord?> CreateIfAbsentAsync(
        JobScheduleRecord schedule,
        CancellationToken cancellationToken);

    ValueTask<JobScheduleRecord> UpsertAsync(
        JobScheduleRecord schedule,
        CancellationToken cancellationToken);

    ValueTask<JobScheduleRecord?> GetAsync(
        string scheduleId,
        CancellationToken cancellationToken);

    ValueTask<bool> SetEnabledAsync(
        string scheduleId,
        bool enabled,
        DateTimeOffset? nextFireAt,
        CancellationToken cancellationToken);

    ValueTask<bool> DeleteAsync(
        string scheduleId,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<ClaimedSchedule>> ClaimDueAsync(
        DateTimeOffset now,
        TimeSpan claimDuration,
        int batchSize,
        CancellationToken cancellationToken);

    /// <summary>
    /// Atomically advances the schedule and optionally creates its logical run and outbox event.
    /// Returns null when no run was created because the occurrence was skipped.
    /// </summary>
    ValueTask<JobRunRecord?> CommitFireAsync(
        CommitScheduleFireCommand command,
        CancellationToken cancellationToken);

    ValueTask ReleaseClaimAsync(
        string scheduleId,
        string claimToken,
        DateTimeOffset retryAt,
        CancellationToken cancellationToken);
}

public interface IOutboxStore
{
    /// <summary>
    /// Claims messages for publication. A message in Publishing state becomes claimable
    /// again after <paramref name="claimDuration"/>, allowing recovery after publisher crashes.
    /// </summary>
    ValueTask<IReadOnlyList<OutboxMessageRecord>> ClaimPendingAsync(
        DateTimeOffset now,
        TimeSpan claimDuration,
        int batchSize,
        CancellationToken cancellationToken);

    ValueTask MarkPublishedAsync(
        string messageId,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken);

    ValueTask MarkFailedAsync(
        string messageId,
        string error,
        DateTimeOffset nextAttemptAt,
        CancellationToken cancellationToken);
}

public interface IWorkAvailableNotifier
{
    ValueTask PublishAsync(
        string queue,
        string payloadJson,
        CancellationToken cancellationToken);
}
