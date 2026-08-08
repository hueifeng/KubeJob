using System.Text.Json;

namespace KubeJob.Core.Runtime;

/// <summary>
/// A non-authoritative hint that a KubeJob logical queue may have claimable
/// PostgresManaged work. Workers must still claim from the control plane before
/// executing anything; this signal never carries execution authority, ordering
/// partitions, or worker-eligibility dimensions.
/// </summary>
public sealed record WorkAvailableSignal
{
    public int SchemaVersion { get; init; }
    public required string EventId { get; init; }
    public required string Queue { get; init; }
    public required string RunId { get; init; }

    public const int CurrentSchemaVersion = 2;
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    public static WorkAvailableSignal FromOutbox(OutboxMessageRecord message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!string.Equals(message.EventType, OutboxEventTypes.WorkAvailable, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Outbox message '{message.Id}' is not a work-available signal.");
        }

        var payload = JsonSerializer.Deserialize<WorkAvailableSignalPayload>(
            message.PayloadJson,
            SerializerOptions)
            ?? throw new InvalidOperationException(
                $"Outbox message '{message.Id}' does not contain a work-available payload.");
        if (string.IsNullOrWhiteSpace(payload.RunId)
            || string.IsNullOrWhiteSpace(payload.Queue)
            || !string.Equals(payload.Queue, message.Queue, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Outbox message '{message.Id}' contains an invalid work-available payload.");
        }

        return new WorkAvailableSignal
        {
            SchemaVersion = CurrentSchemaVersion,
            EventId = message.Id,
            Queue = message.Queue,
            RunId = payload.RunId
        };
    }

    private sealed record WorkAvailableSignalPayload(string RunId, string Queue);
}

/// <summary>
/// Publishes asynchronous work-available hints. Implementations can use an
/// in-process signal, RabbitMQ, Kafka, NATS, a cloud bus, or another broker.
/// Publication never grants execution ownership.
/// </summary>
public interface IWorkAvailableNotifier
{
    ValueTask PublishAsync(
        WorkAvailableSignal signal,
        CancellationToken cancellationToken);
}
