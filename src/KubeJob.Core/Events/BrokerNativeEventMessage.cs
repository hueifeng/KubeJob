namespace KubeJob.Core.Events;

/// <summary>
/// Self-contained event envelope copied independently to every subscription by
/// the transport. No PostgreSQL lookup is required to dispatch or execute it.
/// </summary>
public sealed record BrokerNativeEventMessage
{
    public const int CurrentSchemaVersion = 1;

    public int SchemaVersion { get; init; } = CurrentSchemaVersion;

    public required string EventId { get; init; }

    public required string Topic { get; init; }

    public required string RoutingKey { get; init; }

    public required string PayloadJson { get; init; }

    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;

    public int Attempt { get; init; } = 1;

    public int MaxAttempts { get; init; } = 3;

    public int TimeoutSeconds { get; init; } = 300;

    /// <summary>Optional per-message backoff; null uses the transport default.</summary>
    public Runtime.RetryPolicy? RetryPolicy { get; init; }

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
                $"Unsupported BrokerNative event schema version {SchemaVersion}; expected {CurrentSchemaVersion}.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(EventId);
        ArgumentException.ThrowIfNullOrWhiteSpace(Topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(RoutingKey);
        ArgumentNullException.ThrowIfNull(PayloadJson);

        if (Attempt < 1 || MaxAttempts < 1 || Attempt > MaxAttempts)
        {
            throw new InvalidOperationException("BrokerNative event attempt values are invalid.");
        }

        if (TimeoutSeconds < 1)
        {
            throw new InvalidOperationException("BrokerNative event TimeoutSeconds must be positive.");
        }

        RetryPolicy?.Validate();
    }
}
