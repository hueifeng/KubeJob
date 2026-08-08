namespace KubeJob.Core.Client;

/// <summary>
/// Identifies one submitted Job. For PostgresManaged, <see cref="JobId"/> is a
/// durable Run id. For BrokerNative, it is the transport MessageId and does not
/// imply that a durable KubeJob Run/status record exists.
/// </summary>
/// <param name="BatchId">
/// Reserved for the future durable JobBatch aggregate. The current
/// EnqueueBatchAsync API returns independent Jobs per item and leaves this
/// value null.
/// </param>
public sealed record JobHandle(
    string JobId,
    string? BatchId = null);
