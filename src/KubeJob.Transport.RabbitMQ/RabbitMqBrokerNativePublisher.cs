using KubeJob.Core.Transport;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of the transport-neutral publisher contract.
/// Runtime code supplies logical Queue/Topic destinations; this adapter owns
/// RabbitMQ exchanges, bindings, durability and publisher confirms.
/// </summary>
public sealed class RabbitMqBrokerNativePublisher : IMessageTransportBatchPublisher, IDisposable
{
    public const string Id = "rabbitmq";

    private readonly RabbitMqBrokerNativeOptions _options;
    private readonly object _gate = new();
    private readonly HashSet<string> _declaredJobQueues = new(StringComparer.Ordinal);
    private readonly HashSet<string> _declaredEventTopics = new(StringComparer.Ordinal);
    private IConnection? _connection;
    private IModel? _channel;
    private bool _disposed;

    public RabbitMqBrokerNativePublisher(IOptions<RabbitMqBrokerNativeOptions> options)
    {
        _options = options.Value;
        _options.Validate();
    }

    public string TransportId => Id;

    public MessageTransportCapabilities Capabilities =>
        MessageTransportCapabilities.DurablePublish
        | MessageTransportCapabilities.DeadLetter
        | MessageTransportCapabilities.ConsumerGroups;

    public ValueTask PublishAsync(
        TransportPublishRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return PublishBatchAsync(new[] { request }, cancellationToken);
    }

    public ValueTask PublishBatchAsync(
        IReadOnlyList<TransportPublishRequest> requests,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(requests);
        if (requests.Count == 0)
        {
            return ValueTask.CompletedTask;
        }

        ValidateRequests(requests, cancellationToken);

        lock (_gate)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            EnsureChannel();
            PublishBatchLocked(requests, cancellationToken);
        }

        return ValueTask.CompletedTask;
    }

    private void PublishBatchLocked(
        IReadOnlyList<TransportPublishRequest> requests,
        CancellationToken cancellationToken)
    {
        var channel = _channel!;
        DeclareTopology(channel, requests);

        var mandatoryMessageIds = requests
            .Where(request => request.Kind == TransportMessageKind.Job)
            .Select(request => request.Message.MessageId)
            .ToHashSet(StringComparer.Ordinal);
        var returnedMessageIds = new HashSet<string>(StringComparer.Ordinal);
        var returnedGate = new object();

        EventHandler<BasicReturnEventArgs> returnHandler = (_, args) =>
        {
            var messageId = args.BasicProperties.MessageId;
            if (!string.IsNullOrWhiteSpace(messageId)
                && mandatoryMessageIds.Contains(messageId))
            {
                lock (returnedGate)
                {
                    returnedMessageIds.Add(messageId);
                }
            }
        };

        channel.BasicReturn += returnHandler;
        try
        {
            foreach (var request in requests)
            {
                cancellationToken.ThrowIfCancellationRequested();
                PublishUnconfirmed(channel, request);
            }

            if (!channel.WaitForConfirms(_options.PublisherConfirmTimeout))
            {
                InvalidateChannel();
                throw new IOException(
                    $"RabbitMQ did not confirm a BrokerNative publish batch of {requests.Count} message(s).");
            }

            string[] returned;
            lock (returnedGate)
            {
                returned = returnedMessageIds
                    .OrderBy(id => id, StringComparer.Ordinal)
                    .ToArray();
            }

            if (returned.Length > 0)
            {
                throw new IOException(
                    "RabbitMQ could not route BrokerNative message(s): " +
                    string.Join(",", returned));
            }
        }
        catch
        {
            if (_channel is { IsOpen: false })
            {
                InvalidateChannel();
            }

            throw;
        }
        finally
        {
            if (channel.IsOpen)
            {
                channel.BasicReturn -= returnHandler;
            }
        }
    }

    private void DeclareTopology(
        IModel channel,
        IReadOnlyList<TransportPublishRequest> requests)
    {
        var newJobQueues = requests
            .Where(request => request.Kind == TransportMessageKind.Job)
            .Select(request => Core.Queues.LogicalQueueName.Normalize(
                request.Destination,
                nameof(request.Destination)))
            .Distinct(StringComparer.Ordinal)
            .Where(queue => !_declaredJobQueues.Contains(queue))
            .ToArray();

        if (newJobQueues.Length > 0)
        {
            RabbitMqBrokerNativeTopology.Declare(channel, _options, newJobQueues);
            _declaredJobQueues.UnionWith(newJobQueues);
        }

        foreach (var topic in requests
                     .Where(request => request.Kind == TransportMessageKind.Event)
                     .Select(request => Core.Queues.LogicalQueueName.Normalize(
                         request.Destination,
                         nameof(request.Destination)))
                     .Distinct(StringComparer.Ordinal)
                     .Where(topic => !_declaredEventTopics.Contains(topic)))
        {
            // Publishers own only the Topic exchange. Subscription queues are
            // declared by consumers. No subscription is a valid event topology.
            channel.ExchangeDeclare(
                _options.GetEventExchangeName(topic),
                ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                arguments: null);
            _declaredEventTopics.Add(topic);
        }
    }

    private void PublishUnconfirmed(IModel channel, TransportPublishRequest request)
    {
        string exchange;
        string routingKey;
        bool mandatory;

        switch (request.Kind)
        {
            case TransportMessageKind.Job:
            {
                var logicalQueue = Core.Queues.LogicalQueueName.Normalize(
                    request.Destination,
                    nameof(request.Destination));
                exchange = _options.ExchangeName;
                routingKey = string.IsNullOrWhiteSpace(request.RoutingKey)
                    ? logicalQueue
                    : request.RoutingKey.Trim();
                mandatory = true;
                break;
            }

            case TransportMessageKind.Event:
            {
                var topic = Core.Queues.LogicalQueueName.Normalize(
                    request.Destination,
                    nameof(request.Destination));
                ArgumentException.ThrowIfNullOrWhiteSpace(request.RoutingKey);
                exchange = _options.GetEventExchangeName(topic);
                routingKey = request.RoutingKey.Trim();
                mandatory = false;
                break;
            }

            default:
                throw new InvalidOperationException(
                    $"Unsupported transport message kind '{request.Kind}'.");
        }

        var message = request.Message;
        var properties = channel.CreateBasicProperties();
        properties.Persistent = true;
        properties.ContentType = "application/json";
        properties.Type = message.MessageType;
        properties.MessageId = message.MessageId;
        properties.CorrelationId = message.CorrelationId;

        if (message.Headers is { Count: > 0 })
        {
            properties.Headers = message.Headers.ToDictionary(
                pair => pair.Key,
                pair => (object)pair.Value,
                StringComparer.Ordinal);
        }

        channel.BasicPublish(
            exchange,
            routingKey,
            mandatory,
            properties,
            message.Body);
    }

    private static void ValidateRequests(
        IReadOnlyList<TransportPublishRequest> requests,
        CancellationToken cancellationToken)
    {
        foreach (var request in requests)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ArgumentNullException.ThrowIfNull(request);
            ArgumentNullException.ThrowIfNull(request.Message);

            if (request.NotBefore is not null)
            {
                throw new NotSupportedException(
                    "Generic delayed publish is not enabled for the RabbitMQ BrokerNative transport.");
            }
        }
    }

    private void EnsureChannel()
    {
        if (_connection is { IsOpen: true } && _channel is { IsOpen: true })
        {
            return;
        }

        InvalidateChannel();

        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        _connection = factory.CreateConnection("KubeJob.BrokerNative.Publisher");
        _channel = _connection.CreateModel();
        _channel.ConfirmSelect();
    }

    private void InvalidateChannel()
    {
        try
        {
            _channel?.Dispose();
        }
        catch
        {
            // Best effort cleanup before reconnect.
        }

        try
        {
            _connection?.Dispose();
        }
        catch
        {
            // Best effort cleanup before reconnect.
        }

        _channel = null;
        _connection = null;
        _declaredJobQueues.Clear();
        _declaredEventTopics.Clear();
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            InvalidateChannel();
        }
    }
}
