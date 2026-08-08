namespace KubeJob.Core.Runtime;

/// <summary>
/// Self-contained executable message for BrokerNative queues.
/// A worker must be able to execute this contract without looking up a Run,
/// Attempt, lease, payload, or routing decision in the control-plane database.
/// </summary>
public sealed record BrokerNativeJobMessage
{
    public const int CurrentSchemaVersion = 1;

    public int SchemaVersion { get; init; } = CurrentSchemaVersion;

    public required string MessageId { get; init; }

    public required string JobKey { get; init; }

    public required string Queue { get; init; }

    public required string PayloadJson { get; init; }

    public DateTimeOffset EnqueuedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Current execution attempt, starting at 1. Broker retry republishes a
    /// new message with this value incremented after the new publication is
    /// confirmed and before the original delivery is ACKed.
    /// </summary>
    public int Attempt { get; init; } = 1;

    /// <summary>Total execution attempts including the initial delivery.</summary>
    public int MaxAttempts { get; init; } = 3;

    public int TimeoutSeconds { get; init; } = 300;

    /// <summary>
    /// Optional stable key used only when the configured broker topology needs
    /// partitioned/key-ordered routing. Parallel queues leave it null.
    /// </summary>
    public string? PartitionKey { get; init; }

    public string? IdempotencyKey { get; init; }

    public string? CorrelationId { get; init; }

    public string? TraceParent { get; init; }

    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    public void Validate()
    {
        if (SchemaVersion != CurrentSchemaVersion)
        {
            throw new InvalidOperationException(
                $"Unsupported BrokerNative job schema version {SchemaVersion}; expected {CurrentSchemaVersion}.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(MessageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(JobKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(Queue);
        ArgumentNullException.ThrowIfNull(PayloadJson);

        if (Attempt < 1)
        {
            throw new InvalidOperationException("BrokerNative job Attempt must be at least 1.");
        }

        if (MaxAttempts < 1)
        {
            throw new InvalidOperationException("BrokerNative job MaxAttempts must be at least 1.");
        }

        if (Attempt > MaxAttempts)
        {
            throw new InvalidOperationException(
                "BrokerNative job Attempt cannot exceed MaxAttempts.");
        }

        if (TimeoutSeconds < 1)
        {
            throw new InvalidOperationException(
                "BrokerNative job TimeoutSeconds must be positive.");
        }
    }
}
