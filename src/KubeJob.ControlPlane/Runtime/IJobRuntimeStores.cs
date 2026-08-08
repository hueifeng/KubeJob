using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.ControlPlane.Runtime;

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
    DateTimeOffset? ScheduledFor = null,
    DeliveryTarget? DeliveryTarget = null,
    RetryPolicy? RetryPolicy = null,
    Continuation? Continuation = null,
    Compensation? Compensation = null);

public sealed record SubmitJobResult(JobRunRecord Run, bool Existing);

/// <summary>
/// Result of a cancel request. Cancellation is authoritative durable state;
/// workers observe it through the normal managed lease/heartbeat path.
/// </summary>
public sealed record CancelJobResult(bool Requested);

public interface IJobSubmissionStore
{
    ValueTask<SubmitJobResult> SubmitAsync(
        SubmitJobCommand command,
        CancellationToken cancellationToken);

    /// <summary>
    /// Submits multiple jobs in a single database transaction, amortizing round
    /// trips and WAL flushes. Per-command results preserve idempotency: a command
    /// whose <see cref="SubmitJobCommand.IdempotencyKey"/> already exists returns
    /// that existing run with <c>Existing: true</c> and writes no outbox row.
    /// Implementations must preflight conflicts and leave no new rows behind if
    /// any command cannot be accepted.
    /// </summary>
    ValueTask<IReadOnlyList<SubmitJobResult>> SubmitBatchAsync(
        IReadOnlyList<SubmitJobCommand> commands,
        CancellationToken cancellationToken);

    ValueTask<CancelJobResult> RequestCancelAsync(
        string runId,
        string? reason,
        CancellationToken cancellationToken);

    ValueTask<JobRunRecord?> GetByIdempotencyKeyAsync(
        string idempotencyKey,
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
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<CompleteAttemptResponse>> CompleteBatchAsync(
        IReadOnlyList<CompleteAttemptRequest> requests,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken);

    ValueTask<int> RequeueExpiredLeasesAsync(
        DateTimeOffset now,
        RetryPolicy retryPolicy,
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
        long? expectedVersion,
        CancellationToken cancellationToken);

    ValueTask<bool> DeleteAsync(
        string scheduleId,
        long? expectedVersion,
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

public sealed class PermanentOutboxException : Exception
{
    public PermanentOutboxException(string message)
        : base(message)
    {
    }

    public PermanentOutboxException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

public interface IJobRuntimeMaintenanceStore
{
    ValueTask<int> DeletePublishedOutboxAsync(
        DateTimeOffset olderThan,
        int batchSize,
        CancellationToken cancellationToken);

    /// <summary>
    /// Deletes only terminal runs without an idempotency key or schedule
    /// identity. Keyed terminal history is retained by design: the idempotency
    /// key must never be reused while its historical Run still exists.
    /// </summary>
    ValueTask<int> DeleteUnkeyedTerminalRunsAsync(
        DateTimeOffset olderThan,
        int batchSize,
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
        IReadOnlyList<OutboxPublication> publications,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken);

    ValueTask MarkFailedAsync(
        OutboxFailure failure,
        CancellationToken cancellationToken);

    ValueTask MarkAbandonedAsync(
        OutboxFailure failure,
        CancellationToken cancellationToken);

    /// <summary>
    /// Claims up to <paramref name="batchSize"/> ready outbox messages and
    /// dispatches them one at a time. Each successful dispatch is committed to
    /// the store before the next message is processed, so a failure on one
    /// message does not revert messages that already succeeded. Returns the
    /// identifiers that were dispatched successfully and the identifiers whose
    /// dispatch failed (the latter are already marked Failed in the store).
    /// </summary>
    ValueTask<OutboxDispatchBatch> DispatchOnceAsync(
        TimeSpan claimDuration,
        TimeSpan retryDelay,
        int batchSize,
        Func<OutboxMessageRecord, CancellationToken, ValueTask> dispatch,
        CancellationToken cancellationToken);
}

public sealed record OutboxPublication(string MessageId, string ClaimToken);

public sealed record OutboxFailure(
    string MessageId,
    string ClaimToken,
    string Error,
    DateTimeOffset NextAttemptAt);

/// <summary>
/// Reports the durable outcome of a single <see cref="IOutboxStore.DispatchOnceAsync"/>
/// call. The store marks each row Published or Failed inside its own transaction,
/// so partial-batch failures cannot leak already-published rows back to a
/// subsequent poll cycle.
/// </summary>
public sealed record OutboxDispatchBatch(
    IReadOnlyList<string> DispatchedIds,
    IReadOnlyList<string> FailedIds,
    IReadOnlyList<string>? AbandonedIds = null)
{
    public IReadOnlyList<string> Abandoned => AbandonedIds ?? Array.Empty<string>();
}
