namespace KubeJob.Core.Transport;

/// <summary>
/// Capabilities exposed by a broker adapter. Runtime code uses these flags to
/// reject unsupported semantics instead of silently degrading them.
/// </summary>
[Flags]
public enum MessageTransportCapabilities
{
    None = 0,
    DurablePublish = 1 << 0,
    DelayedDelivery = 1 << 1,
    DeadLetter = 1 << 2,
    ConsumerGroups = 1 << 3,
    Partitioning = 1 << 4,
    Replay = 1 << 5,
    OrderedDelivery = 1 << 6
}

public enum TransportMessageKind
{
    Job = 0,
    Event = 1
}

/// <summary>
/// Transport-neutral wire payload. Broker-specific delivery tags, offsets,
/// channels, exchanges, and queue handles deliberately do not belong here.
/// </summary>
public sealed record TransportMessage(
    string MessageId,
    string MessageType,
    ReadOnlyMemory<byte> Body,
    IReadOnlyDictionary<string, string>? Headers = null,
    string? CorrelationId = null,
    string? PartitionKey = null);

/// <summary>
/// Logical publish request. Destination is a KubeJob Queue for jobs and a
/// KubeJob Topic for events. A transport adapter maps it to its physical
/// topology (RabbitMQ exchange/queue, Kafka topic/group, SQS queue, etc.).
/// </summary>
public sealed record TransportPublishRequest(
    TransportMessageKind Kind,
    string Destination,
    TransportMessage Message,
    string? RoutingKey = null,
    DateTimeOffset? NotBefore = null);

public interface IMessageTransportPublisher
{
    string TransportId { get; }

    MessageTransportCapabilities Capabilities { get; }

    ValueTask PublishAsync(
        TransportPublishRequest request,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Optional high-throughput extension for transports that can amortize durable
/// publish acknowledgement across multiple messages. Implementing this
/// interface does not make a batch atomic: a broker/network failure may leave a
/// confirmed prefix published. The benefit is fewer transport round trips.
/// </summary>
public interface IMessageTransportBatchPublisher : IMessageTransportPublisher
{
    ValueTask PublishBatchAsync(
        IReadOnlyList<TransportPublishRequest> requests,
        CancellationToken cancellationToken = default);
}

public interface IMessageTransportRegistry
{
    IMessageTransportPublisher GetRequiredPublisher(string transportId);

    bool TryGetPublisher(
        string transportId,
        out IMessageTransportPublisher? publisher);
}

/// <summary>
/// Registry over all installed transport adapters. Multiple brokers may be
/// installed in one process and selected independently per logical Queue.
/// </summary>
public sealed class MessageTransportRegistry : IMessageTransportRegistry
{
    private readonly IReadOnlyDictionary<string, IMessageTransportPublisher> _publishers;

    public MessageTransportRegistry(IEnumerable<IMessageTransportPublisher> publishers)
    {
        ArgumentNullException.ThrowIfNull(publishers);

        var map = new Dictionary<string, IMessageTransportPublisher>(StringComparer.OrdinalIgnoreCase);
        foreach (var publisher in publishers)
        {
            ArgumentNullException.ThrowIfNull(publisher);
            ArgumentException.ThrowIfNullOrWhiteSpace(publisher.TransportId);

            if (!map.TryAdd(publisher.TransportId.Trim(), publisher))
            {
                throw new InvalidOperationException(
                    $"Multiple message transports are registered with id '{publisher.TransportId}'.");
            }
        }

        _publishers = map;
    }

    public IMessageTransportPublisher GetRequiredPublisher(string transportId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(transportId);
        if (_publishers.TryGetValue(transportId.Trim(), out var publisher))
        {
            return publisher;
        }

        throw new InvalidOperationException(
            $"Message transport '{transportId}' is not registered. " +
            "Install and register a transport adapter before routing a BrokerNative queue to it.");
    }

    public bool TryGetPublisher(
        string transportId,
        out IMessageTransportPublisher? publisher)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(transportId);
        return _publishers.TryGetValue(transportId.Trim(), out publisher);
    }
}
