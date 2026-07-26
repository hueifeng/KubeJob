namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// JSON contract consumed by the RabbitMQ business-message adapter. The
/// broker MessageId property takes precedence; MessageId here supports
/// producers that carry identity only in the body.
/// </summary>
public sealed record RabbitMqJobIngressEnvelope(
    string MessageId,
    string JobKey,
    string PayloadJson,
    string Queue = "default",
    int Priority = 0,
    DateTimeOffset? NotBefore = null,
    string? ConcurrencyKey = null,
    int MaxAttempts = 1,
    int TimeoutSeconds = 300);
