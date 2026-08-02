namespace KubeJob.Core.Client;

/// <summary>
/// Identifies a submitted logical job run.
/// </summary>
/// <param name="BatchId">
/// Reserved for the future durable JobBatch aggregate. The current
/// EnqueueBatchAsync API returns one independent Run per item and leaves this
/// value null.
/// </param>
public sealed record JobHandle(
    string JobId,
    string? BatchId = null);
