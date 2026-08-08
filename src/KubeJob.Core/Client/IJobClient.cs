using KubeJob.Core.Jobs;

namespace KubeJob.Core.Client;

/// <summary>
/// Submits and observes logical background jobs without exposing transport or storage details.
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
    /// Submits multiple jobs of the same type through one control-plane batch.
    /// The server validates every item before opening the store transaction and
    /// preserves input order in the returned handles. This is a bounded,
    /// atomic batch optimization; it is not a durable JobBatch aggregate
    /// with independent lifecycle or MaxParallelism semantics.
    /// </summary>
    ValueTask<IReadOnlyList<JobHandle>> EnqueueBatchAsync<TPayload>(
        JobKey<TPayload> job,
        IReadOnlyList<(TPayload Payload, JobEnqueueOptions? Options)> batch,
        CancellationToken cancellationToken = default);

    ValueTask<JobStatusSnapshot?> GetStatusAsync(
        string jobId,
        CancellationToken cancellationToken = default);

    ValueTask<bool> CancelAsync(
        string jobId,
        string? reason = null,
        CancellationToken cancellationToken = default);
}
