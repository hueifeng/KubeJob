using System.Text.Json;

namespace KubeJob.Core.Runtime;

/// <summary>
/// A non-authoritative hint that a KubeJob queue may have claimable work.
/// Workers must still claim from the control plane before executing anything.
/// </summary>
/// <remarks>
/// <see cref="PartitionKey"/> threads the run's ConcurrencyKey through to the
/// transport adapter so same-key runs can co-locate on a physical lane queue;
/// a null value resolves to lane 0.
/// </remarks>
public sealed record WorkAvailableSignal
{
    public int SchemaVersion { get; init; }
    public required string EventId { get; init; }
    public required string Queue { get; init; }
    public required string ExecutionLane { get; init; }
    public required string ConsumerGroup { get; init; }
    public required string RunId { get; init; }
    public string? PartitionKey { get; init; }

    public const int CurrentSchemaVersion = 1;
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    public static WorkAvailableSignal FromOutbox(OutboxMessageRecord message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!string.Equals(message.EventType, "work-available", StringComparison.Ordinal))
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
            ExecutionLane = message.ExecutionLane,
            RunId = payload.RunId,
            ConsumerGroup = message.ConsumerGroup,
            PartitionKey = message.PartitionKey
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
