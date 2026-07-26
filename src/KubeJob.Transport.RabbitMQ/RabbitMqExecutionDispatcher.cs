using System.Text;
using System.Text.Json;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Publishes durable execution envelopes. The envelope identifies a logical
/// Run, but never grants execution authority; the consumer must perform
/// targeted control-plane Admission before invoking a handler.
/// </summary>
public sealed class RabbitMqExecutionDispatcher : IExecutionDispatcher, IDisposable
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly RabbitMqExecutionOptions _options;
    private readonly object _gate = new();
    private IConnection? _connection;
    private IModel? _channel;

    public RabbitMqExecutionDispatcher(IOptions<RabbitMqExecutionOptions> options)
    {
        _options = options.Value;
        _options.Validate();
    }

    public ValueTask DispatchAsync(
        ExecutionEnvelope envelope,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(envelope);
        if (Encoding.UTF8.GetByteCount(envelope.Queue) >= 255)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution routing keys must be shorter than 255 UTF-8 bytes.");
        }

        lock (_gate)
        {
            try
            {
                var channel = EnsureChannel();
                var properties = channel.CreateBasicProperties();
                properties.ContentType = "application/json";
                properties.DeliveryMode = 2;
                properties.Type = "execution-envelope";
                properties.MessageId = envelope.EventId;

                channel.BasicPublish(
                    exchange: _options.ExchangeName,
                    routingKey: envelope.Queue,
                    mandatory: false,
                    basicProperties: properties,
                    body: Encoding.UTF8.GetBytes(
                        JsonSerializer.Serialize(envelope, SerializerOptions)));

                if (!channel.WaitForConfirms(_options.PublisherConfirmTimeout))
                {
                    throw new IOException(
                        $"RabbitMQ did not confirm execution envelope '{envelope.EventId}' " +
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

        _connection = factory.CreateConnection("KubeJob.ExecutionDispatcher");
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
