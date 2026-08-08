using KubeJob.Core.Runtime;

namespace KubeJob.Core.Client;

/// <summary>
/// Identifies a submitted KubeJob job. For PostgresManaged this identifier is a
/// durable logical Run id; for BrokerNative it is the broker message id.
/// </summary>
/// <param name="BatchId">
/// Reserved for the future durable JobBatch aggregate. The current
/// EnqueueBatchAsync API returns one independent job per item and leaves this
/// value null.
/// </param>
public sealed record JobHandle(
    string JobId,
    string? BatchId = null)
{
    /// <summary>The execution authority selected for this submission.</summary>
    public QueueRuntimeMode RuntimeMode { get; init; } = QueueRuntimeMode.PostgresManaged;

    /// <summary>
    /// Physical transport adapter id for BrokerNative submissions. Managed jobs
    /// leave this null because PostgreSQL is their execution authority.
    /// </summary>
    public string? TransportId { get; init; }

    /// <summary>
    /// True when <see cref="IJobClient.GetStatusAsync(string, CancellationToken)"/>
    /// observes a durable strongly-consistent Run lifecycle for this handle.
    /// </summary>
    public bool SupportsStrongStatus => RuntimeMode == QueueRuntimeMode.PostgresManaged;

    /// <summary>
    /// True when <see cref="IJobClient.CancelAsync(string, string?, CancellationToken)"/>
    /// provides KubeJob's durable managed cancellation contract.
    /// </summary>
    public bool SupportsStrongCancellation => RuntimeMode == QueueRuntimeMode.PostgresManaged;
}
