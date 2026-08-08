using System.Collections.Concurrent;
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

        foreach (var request in requests)
        {
            ValidateRequest(request);
        }

        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            EnsureChannel();

            var channel = _channel!;
            var returnedMessageIds = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);

            EventHandler<BasicReturnEventArgs> returnHandler = (_, args) =>
            {
                var messageId = args.BasicProperties.MessageId;
                if (!string.IsNullOrWhiteSpace(messageId))
                {
                    // BasicReturn is delivered by the RabbitMQ connection
                    // dispatch thread, not necessarily the caller that is
                    // waiting for confirms, so this collection must be
                    // thread-safe.
                    returnedMessageIds.TryAdd(messageId, 0);
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

                // RabbitMQ confirms are channel-ordered. Publishing the whole
                // application batch first and waiting once removes one broker
                // round trip per message while preserving publisher-confirm
                // durability semantics. This is not an atomic transaction: a
                // failure can still happen after RabbitMQ accepted a subset.
                if (!channel.WaitForConfirms(_options.PublisherConfirmTimeout))
                {
                    InvalidateChannel();
                    throw new IOException(
                        $"RabbitMQ did not confirm a BrokerNative publish batch of {requests.Count} message(s).");
                }

                if (!returnedMessageIds.IsEmpty)
                {
                    // A durable Job queue may have been deleted/reconfigured
                    // outside this process after it was cached as declared.
                    // Rebuild the channel so the next retry re-declares the
                    // physical topology instead of repeatedly trusting a stale
                    // cache entry.
                    InvalidateChannel();
                    throw new IOException(
                        $"RabbitMQ could not route {returnedMessageIds.Count} BrokerNative message(s): " +
                        string.Join(",", returnedMessageIds.Keys.Take(8)) +
                        (returnedMessageIds.Count > 8 ? ",..." : string.Empty));
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

        return ValueTask.CompletedTask;
    }

    private static void ValidateRequest(TransportPublishRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Message);
        if (request.NotBefore is not null)
        {
            throw new NotSupportedException(
                "Generic delayed publish is not enabled for the RabbitMQ BrokerNative transport.");
        }
    }

    private void PublishUnconfirmed(
        IModel channel,
        TransportPublishRequest request)
    {
        switch (request.Kind)
        {
            case TransportMessageKind.Job:
            {
                var logicalQueue = Core.Queues.LogicalQueueName.Normalize(
                    request.Destination,
                    nameof(request.Destination));
                if (!_declaredJobQueues.Contains(logicalQueue))
                {
                    // Queue/exchange declaration is synchronous broker RPC.
                    // Cache only successful declarations for this channel
                    // lifetime instead of paying that RTT on every message.
                    RabbitMqBrokerNativeTopology.Declare(
                        channel,
                        _options,
                        new[] { logicalQueue });
                    _declaredJobQueues.Add(logicalQueue);
                }

                var routingKey = string.IsNullOrWhiteSpace(request.RoutingKey)
                    ? logicalQueue
                    : request.RoutingKey.Trim();
                PublishUnconfirmed(
                    channel,
                    _options.ExchangeName,
                    routingKey,
                    mandatory: true,
                    request.Message);
                break;
            }

            case TransportMessageKind.Event:
            {
                var topic = Core.Queues.LogicalQueueName.Normalize(
                    request.Destination,
                    nameof(request.Destination));
                ArgumentException.ThrowIfNullOrWhiteSpace(request.RoutingKey);
                var exchange = _options.GetEventExchangeName(topic);
                if (!_declaredEventTopics.Contains(topic))
                {
                    // Publishers own only the Topic exchange. Subscription
                    // queues are declared by consumers. Zero subscribers is a
                    // valid pub/sub topology, so event publication is not
                    // mandatory and does not manufacture a queue.
                    channel.ExchangeDeclare(
                        exchange,
                        ExchangeType.Topic,
                        durable: true,
                        autoDelete: false,
                        arguments: null);
                    _declaredEventTopics.Add(topic);
                }

                PublishUnconfirmed(
                    channel,
                    exchange,
                    request.RoutingKey.Trim(),
                    mandatory: false,
                    request.Message);
                break;
            }

            default:
                throw new InvalidOperationException(
                    $"Unsupported transport message kind '{request.Kind}'.");
        }
    }

    private static void PublishUnconfirmed(
        IModel channel,
        string exchange,
        string routingKey,
        bool mandatory,
        TransportMessage message)
    {
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
