namespace KubeJob.Core.Runtime;

/// <summary>
/// A business message received from an external broker. MessageId is the
/// broker's stable delivery identity, not a KubeJob Run or Attempt identity.
/// </summary>
public sealed record JobIngressMessage(
    string Source,
    string MessageId,
    EnqueueJobRequest Job);

public sealed record JobIngressResult(
    string JobId,
    bool Existing);

/// <summary>
/// Durable hand-off seam for RabbitMQ, Kafka, NATS, or application-specific
/// consumers. Implementations must return only after the logical Run is
/// durably accepted or a permanent validation/idempotency error is known.
/// </summary>
public interface IJobMessageIngress
{
    ValueTask<JobIngressResult> SubmitAsync(
        JobIngressMessage message,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Batch-capable form of <see cref="IJobMessageIngress"/>. Results preserve
/// input order and are returned only after every accepted Run is durable.
/// </summary>
public interface IJobMessageIngressBatch : IJobMessageIngress
{
    ValueTask<IReadOnlyList<JobIngressResult>> SubmitBatchAsync(
        IReadOnlyList<JobIngressMessage> messages,
        CancellationToken cancellationToken = default);
}
