namespace KubeJob.Core.Events;

/// <summary>
/// Durable idempotency boundary for broker-delivered events. The consumer name
/// is the fixed delivery capability (log, data, or notify), not a worker id.
/// </summary>
public interface IEventInboxStore
{
    ValueTask<bool> IsProcessedAsync(
        string eventId,
        string consumerName,
        CancellationToken cancellationToken = default);

    ValueTask MarkProcessedAsync(
        string eventId,
        string consumerName,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Prevents an event consumer from silently running without the durable Inbox
/// required for at-least-once broker delivery.
/// </summary>
public sealed class MissingEventInboxStore : IEventInboxStore
{
    private static InvalidOperationException CreateException() => new(
        "A durable event Inbox is required. Configure AddKubeJobPostgreSql before starting an Event consumer.");

    public ValueTask<bool> IsProcessedAsync(
        string eventId,
        string consumerName,
        CancellationToken cancellationToken = default)
        => ValueTask.FromException<bool>(CreateException());

    public ValueTask MarkProcessedAsync(
        string eventId,
        string consumerName,
        CancellationToken cancellationToken = default)
        => ValueTask.FromException(CreateException());
}
