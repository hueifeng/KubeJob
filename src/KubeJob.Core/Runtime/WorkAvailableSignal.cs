using System.Text.Json;

namespace KubeJob.Core.Runtime;

/// <summary>
/// A non-authoritative hint that a KubeJob queue may have claimable work.
/// Workers must still claim from the control plane before executing anything.
/// A signal may represent one of many coalesced Runs on the same Queue.
/// </summary>
public sealed record WorkAvailableSignal
{
    public int SchemaVersion { get; init; }
    public required string EventId { get; init; }
    public required string Queue { get; init; }
    public required string ExecutionLane { get; init; }
    public required string ConsumerGroup { get; init; }
    public required string RunId { get; init; }

    /// <summary>
    /// Optional diagnostic/routing metadata copied from the Run's concurrency
    /// key. It does not grant ordering or execution ownership.
    /// </summary>
    public string? PartitionKey { get; init; }

    public const int CurrentSchemaVersion = 1;
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Creates a process-local best-effort wake after a durable managed Run has
    /// committed. EventId is intentionally ephemeral because correctness never
    /// depends on durable delivery of this signal.
    /// </summary>
    public static WorkAvailableSignal ForRun(JobRunRecord run)
    {
        ArgumentNullException.ThrowIfNull(run);
        return new WorkAvailableSignal
        {
            SchemaVersion = CurrentSchemaVersion,
            EventId = Guid.NewGuid().ToString("N"),
            Queue = run.Queue,
            ExecutionLane = run.ExecutionLane,
            RunId = run.Id,
            ConsumerGroup = run.ConsumerGroup,
            PartitionKey = run.ConcurrencyKey
        };
    }

    /// <summary>
    /// Rehydrates the compatibility durable wake path used for delayed/recovery
    /// scenarios that still persist WorkAvailable rows in Kj2_Outbox.
    /// </summary>
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
/// Publication never grants execution ownership and may be best effort.
/// </summary>
public interface IWorkAvailableNotifier
{
    ValueTask PublishAsync(
        WorkAvailableSignal signal,
        CancellationToken cancellationToken);
}
