using KubeJob.Core.Jobs;

namespace KubeJob.Core.Client;

/// <summary>
/// Submits KubeJob jobs through the configured Queue execution authority.
/// Strong status and durable cancellation are PostgresManaged capabilities;
/// BrokerNative handles identify transport messages and expose their runtime
/// mode through <see cref="JobHandle.RuntimeMode"/>.
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
    /// Submits multiple jobs of the same type. PostgresManaged batches are
    /// validated and persisted atomically by the state store. BrokerNative
    /// batches are not atomic; a transport that implements batch publishing may
    /// amortize acknowledgement round trips while preserving per-message
    /// delivery semantics. This API is not a durable JobBatch aggregate.
    /// </summary>
    ValueTask<IReadOnlyList<JobHandle>> EnqueueBatchAsync<TPayload>(
        JobKey<TPayload> job,
        IReadOnlyList<(TPayload Payload, JobEnqueueOptions? Options)> batch,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the durable PostgresManaged Run lifecycle. BrokerNative message ids
    /// have no strong Run state in this API and therefore normally return null.
    /// Inspect <see cref="JobHandle.SupportsStrongStatus"/> before relying on
    /// strong observation semantics.
    /// </summary>
    ValueTask<JobStatusSnapshot?> GetStatusAsync(
        string jobId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Requests durable cancellation of a PostgresManaged Run. BrokerNative
    /// queued/active cancellation is not implemented by this operation and a
    /// BrokerNative message id normally returns false.
    /// </summary>
    ValueTask<bool> CancelAsync(
        string jobId,
        string? reason = null,
        CancellationToken cancellationToken = default);
}
