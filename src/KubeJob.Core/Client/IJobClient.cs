using KubeJob.Core.Jobs;

namespace KubeJob.Core.Client;

/// <summary>
/// Submits logical background work while Queue configuration selects the
/// execution authority. PostgresManaged Jobs expose durable status and strong
/// cooperative cancellation; BrokerNative Jobs use at-least-once transport
/// delivery and do not currently have a KubeJob history/cancel projection.
/// </summary>
public interface IJobClient
{
    ValueTask<JobHandle> EnqueueAsync<TPayload>(
        JobKey<TPayload> job,
        TPayload payload,
        CancellationToken cancellationToken = default);

    ValueTask<JobHandle> EnqueueAsync<TPayload>(
        JobKey<TPayload> job,
        TPayload payload,
        JobEnqueueOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Submits multiple jobs of the same type and preserves input order in the
    /// returned handles. PostgresManaged submissions use one bounded database
    /// transaction. BrokerNative submissions may use a transport batch
    /// optimization but are not atomic: a publish failure can occur after a
    /// subset or all messages were accepted. BrokerNative retries therefore
    /// require idempotent business side effects; IdempotencyKey is metadata and
    /// is not an internal BrokerNative deduplication store.
    /// </summary>
    ValueTask<IReadOnlyList<JobHandle>> EnqueueBatchAsync<TPayload>(
        JobKey<TPayload> job,
        IReadOnlyList<(TPayload Payload, JobEnqueueOptions? Options)> batch,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the durable PostgresManaged lifecycle. BrokerNative MessageIds are
    /// not projected into this store in V3 today and therefore normally return
    /// null.
    /// </summary>
    ValueTask<JobStatusSnapshot?> GetStatusAsync(
        string jobId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Requests durable cooperative cancellation for PostgresManaged Jobs.
    /// BrokerNative queued cancellation is not implemented by this API today.
    /// </summary>
    ValueTask<bool> CancelAsync(
        string jobId,
        string? reason = null,
        CancellationToken cancellationToken = default);
}
