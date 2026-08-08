namespace KubeJob.Core.Events;

/// <summary>
/// Deployment-level event Topic routing. Event publishers select a logical
/// Topic only; the deployment chooses the physical broker adapter.
/// </summary>
public sealed class EventRuntimeOptions
{
    public string DefaultTransportId { get; set; } = "rabbitmq";

    public Dictionary<string, string> Topics { get; } = new(StringComparer.Ordinal);

    public string ResolveTransportId(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var normalized = topic.Trim();
        var transportId = Topics.TryGetValue(normalized, out var configured)
            ? configured
            : DefaultTransportId;
        ArgumentException.ThrowIfNullOrWhiteSpace(transportId);
        return transportId.Trim();
    }
}
