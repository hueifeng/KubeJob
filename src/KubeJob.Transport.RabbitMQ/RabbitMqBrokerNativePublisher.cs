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
public sealed class RabbitMqBrokerNativePublisher : IMessageTransportPublisher, IDisposable
{
    public const string Id = "rabbitmq";

    private readonly RabbitMqBrokerNativeOptions _options;
    private readonly object _gate = new();
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
        | MessageTransportCapabilities.ConsumerGroups
        | MessageTransportCapabilities.OrderedDelivery;

    public ValueTask PublishAsync(
        TransportPublishRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Message);
        cancellationToken.ThrowIfCancellationRequested();

        if (request.NotBefore is not null)
        {
            throw new NotSupportedException(
                "Generic delayed publish is not enabled for the RabbitMQ BrokerNative transport.");
        }

        lock (_gate)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            EnsureChannel();

            return request.Kind switch
            {
                TransportMessageKind.Job => PublishJob(request),
                TransportMessageKind.Event => PublishEvent(request),
                _ => throw new InvalidOperationException(
                    $"Unsupported transport message kind '{request.Kind}'.")
            };
        }
    }

    private ValueTask PublishJob(TransportPublishRequest request)
    {
        var logicalQueue = Core.Queues.LogicalQueueName.Normalize(
            request.Destination,
            nameof(request.Destination));
        var routingKey = string.IsNullOrWhiteSpace(request.RoutingKey)
            ? logicalQueue
            : request.RoutingKey.Trim();
        var channel = _channel!;

        RabbitMqBrokerNativeTopology.Declare(
            channel,
            _options,
            new[] { logicalQueue });

        BasicReturnEventArgs? returned = null;
        EventHandler<BasicReturnEventArgs> returnHandler = (_, args) =>
        {
            if (string.Equals(
                    args.BasicProperties.MessageId,
                    request.Message.MessageId,
                    StringComparison.Ordinal))
            {
                returned = args;
            }
        };

        channel.BasicReturn += returnHandler;
        try
        {
            PublishConfirmed(
                channel,
                _options.ExchangeName,
                routingKey,
                mandatory: true,
                request.Message);

            if (returned is not null)
            {
                throw new IOException(
                    $"RabbitMQ could not route BrokerNative message '{request.Message.MessageId}' " +
                    $"to logical queue '{logicalQueue}'.");
            }
        }
        finally
        {
            if (channel.IsOpen)
            {
                channel.BasicReturn -= returnHandler;
            }
        }

        return ValueTask.CompletedTask;
    }

    private ValueTask PublishEvent(TransportPublishRequest request)
    {
        var topic = Core.Queues.LogicalQueueName.Normalize(
            request.Destination,
            nameof(request.Destination));
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RoutingKey);
        var routingKey = request.RoutingKey.Trim();
        var channel = _channel!;
        var exchange = _options.GetEventExchangeName(topic);

        // Publishers own only the Topic exchange. Subscription queues are
        // declared by consumers. No subscription is a valid event topology, so
        // Event publish deliberately does not use mandatory routing.
        channel.ExchangeDeclare(
            exchange,
            ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            arguments: null);

        PublishConfirmed(
            channel,
            exchange,
            routingKey,
            mandatory: false,
            request.Message);
        return ValueTask.CompletedTask;
    }

    private void PublishConfirmed(
        IModel channel,
        string exchange,
        string routingKey,
        bool mandatory,
        TransportMessage message)
    {
        try
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

            if (!channel.WaitForConfirms(_options.PublisherConfirmTimeout))
            {
                InvalidateChannel();
                throw new IOException(
                    $"RabbitMQ did not confirm message '{message.MessageId}'.");
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
