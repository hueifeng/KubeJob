using System.Text;
using System.Text.Json;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Publishes non-authoritative wake-up hints. Durable job state and delivery
/// eligibility remain in the KubeJob state store.
/// </summary>
public sealed class RabbitMqWorkAvailableNotifier : IWorkAvailableNotifier, IDisposable
{
    private readonly RabbitMqNotificationOptions _options;
    private readonly object _gate = new();
    private IConnection? _connection;
    private IModel? _channel;

    public RabbitMqWorkAvailableNotifier(IOptions<RabbitMqNotificationOptions> options)
    {
        _options = options.Value;
        _options.Validate();
    }

    public ValueTask PublishAsync(
        WorkAvailableSignal signal,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(signal);
        if (Encoding.UTF8.GetByteCount(signal.Queue) >= 255)
        {
            throw new InvalidOperationException(
                "RabbitMQ routing keys must be shorter than 255 UTF-8 bytes.");
        }

        lock (_gate)
        {
            try
            {
                var channel = EnsureChannel();
                var properties = channel.CreateBasicProperties();
                properties.ContentType = "application/json";
                properties.DeliveryMode = 1;
                properties.Type = "work-available";

                channel.BasicPublish(
                    exchange: _options.ExchangeName,
                    routingKey: signal.Queue,
                    mandatory: false,
                    basicProperties: properties,
                    body: Encoding.UTF8.GetBytes(JsonSerializer.Serialize(signal)));

                if (!channel.WaitForConfirms(_options.PublisherConfirmTimeout))
                {
                    throw new IOException(
                        $"RabbitMQ did not confirm KubeJob signal '{signal.EventId}' " +
                        $"within {_options.PublisherConfirmTimeout}.");
                }
            }
            catch
            {
                ResetConnection();
                throw;
            }
        }

        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        lock (_gate)
        {
            ResetConnection();
        }
    }

    private IModel EnsureChannel()
    {
        if (_channel is { IsOpen: true })
        {
            return _channel;
        }

        ResetConnection();
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        _connection = factory.CreateConnection("KubeJob.Outbox");
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(
            exchange: _options.ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);
        _channel.ConfirmSelect();
        return _channel;
    }

    private void ResetConnection()
    {
        try
        {
            _channel?.Dispose();
        }
        catch
        {
        }

        try
        {
            _connection?.Dispose();
        }
        catch
        {
        }

        _channel = null;
        _connection = null;
    }
}
