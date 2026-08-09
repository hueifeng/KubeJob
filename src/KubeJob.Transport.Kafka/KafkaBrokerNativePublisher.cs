using Confluent.Kafka;
using KubeJob.Core.Queues;
using KubeJob.Core.Transport;
using Microsoft.Extensions.Options;

namespace KubeJob.Transport.Kafka;

/// <summary>Kafka implementation of the transport-neutral BrokerNative publisher.</summary>
public sealed class KafkaBrokerNativePublisher : IMessageTransportPublisher, IDisposable
{
    public const string Id = "kafka";

    private readonly KafkaBrokerNativeOptions _options;
    private readonly IProducer<string, byte[]> _producer;

    public KafkaBrokerNativePublisher(IOptions<KafkaBrokerNativeOptions> options)
    {
        _options = options.Value;
        _options.Validate();
        _producer = new ProducerBuilder<string, byte[]>(KafkaClientOptions.CreateProducerConfig(_options)).Build();
    }

    public string TransportId => Id;

    public MessageTransportCapabilities Capabilities =>
        MessageTransportCapabilities.DurablePublish |
        MessageTransportCapabilities.DeadLetter |
        MessageTransportCapabilities.ConsumerGroups |
        MessageTransportCapabilities.Partitioning |
        MessageTransportCapabilities.OrderedDelivery |
        MessageTransportCapabilities.Replay;

    public async ValueTask PublishAsync(TransportPublishRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Message);
        cancellationToken.ThrowIfCancellationRequested();

        if (request.NotBefore is not null)
        {
            throw new NotSupportedException(
                "Kafka BrokerNative publish does not accept arbitrary delayed delivery; retry scheduling is adapter-owned.");
        }

        var topic = request.Kind switch
        {
            TransportMessageKind.Job => _options.GetJobTopic(request.Destination),
            TransportMessageKind.Event => GetEventTopic(request.Destination),
            _ => throw new InvalidOperationException($"Unsupported transport message kind '{request.Kind}'.")
        };
        var key = request.Message.PartitionKey
            ?? request.Message.CorrelationId
            ?? request.Message.MessageId;

        await _producer.ProduceAsync(
            topic,
            new Message<string, byte[]>
            {
                Key = key,
                Value = request.Message.Body.ToArray(),
                Headers = KafkaMessageHeaders.Create(request.Message)
            },
            cancellationToken);
    }

    public void Dispose() => _producer.Dispose();

    private string GetEventTopic(string destination)
    {
        _ = LogicalQueueName.Normalize(destination, nameof(destination));
        return _options.EventTopic;
    }
}
