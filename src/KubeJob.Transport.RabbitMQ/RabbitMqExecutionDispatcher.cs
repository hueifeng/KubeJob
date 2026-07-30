using System.Diagnostics;
using System.Text;
using System.Text.Json;
using KubeJob.Core.Runtime;
using KubeJob.Transport.RabbitMQ.Telemetry;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Publishes durable execution envelopes through a bounded pool of independent
/// RabbitMQ channels. RabbitMQ IModel instances are not thread-safe, so each
/// slot owns its connection, channel, confirm wait, and return handler.
/// </summary>
public sealed class RabbitMqExecutionDispatcher : IExecutionTransport, IDisposable
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly RabbitMqExecutionOptions _options;
    private readonly PublisherSlot[] _slots;
    private readonly KubeJobRabbitMqMetrics? _metrics;
    private int _nextSlot;

    public RabbitMqExecutionDispatcher(
        IOptions<RabbitMqExecutionOptions> options,
        KubeJobRabbitMqMetrics? metrics = null)
    {
        _options = options.Value;
        _metrics = metrics;
        _options.Validate();
        _slots = Enumerable.Range(0, _options.PublisherConcurrency)
            .Select(_ => new PublisherSlot())
            .ToArray();
    }

    public string TransportId => _options.TransportId;

    public ValueTask PublishAsync(
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

        var startedAt = _metrics?.IsPublishDurationEnabled == true
            ? Stopwatch.GetTimestamp()
            : 0L;
        var slotIndex = (int)((uint)Interlocked.Increment(ref _nextSlot) % (uint)_slots.Length);
        var slot = _slots[slotIndex];
        lock (slot.Gate)
        {
            try
            {
                var channel = EnsureChannel(slot);
                var properties = channel.CreateBasicProperties();
                properties.ContentType = "application/json";
                properties.DeliveryMode = 2;
                properties.Type = "execution-envelope";
                properties.MessageId = envelope.EventId;

                BasicReturnEventArgs? returned = null;
                EventHandler<BasicReturnEventArgs> returnHandler = (_, args) =>
                {
                    if (string.Equals(args.BasicProperties.MessageId, envelope.EventId, StringComparison.Ordinal))
                    {
                        returned = args;
                    }
                };
                channel.BasicReturn += returnHandler;
                try
                {
                    channel.BasicPublish(
                        exchange: _options.GetGroupExchangeName(),
                        routingKey: envelope.Queue,
                        mandatory: true,
                        basicProperties: properties,
                        body: Encoding.UTF8.GetBytes(
                            JsonSerializer.Serialize(envelope, SerializerOptions)));

                    if (!channel.WaitForConfirms(_options.PublisherConfirmTimeout))
                    {
                        throw new IOException(
                            $"RabbitMQ did not confirm execution envelope '{envelope.EventId}' " +
                            $"within {_options.PublisherConfirmTimeout}.");
                    }

                    if (returned is not null)
                    {
                        throw new IOException(
                            $"RabbitMQ could not route execution envelope '{envelope.EventId}' " +
                            $"with routing key '{envelope.Queue}' (reply code {returned.ReplyCode}: {returned.ReplyText}).");
                    }
                }
                finally
                {
                    channel.BasicReturn -= returnHandler;
                }
            }
            catch
            {
                _metrics?.PublishFailed();
                ResetConnection(slot);
                throw;
            }
        }

        _metrics?.Published(startedAt == 0
            ? TimeSpan.Zero
            : Stopwatch.GetElapsedTime(startedAt));
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        foreach (var slot in _slots)
        {
            lock (slot.Gate)
            {
                ResetConnection(slot);
            }
        }
    }

    private IModel EnsureChannel(PublisherSlot slot)
    {
        if (slot.Channel is { IsOpen: true })
        {
            return slot.Channel;
        }

        ResetConnection(slot);
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        slot.Connection = factory.CreateConnection("KubeJob.ExecutionDispatcher");
        slot.Channel = slot.Connection.CreateModel();
        slot.Channel.ExchangeDeclare(
            exchange: _options.GetGroupExchangeName(),
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);
        slot.Channel.ConfirmSelect();
        return slot.Channel;
    }

    private static void ResetConnection(PublisherSlot slot)
    {
        try
        {
            slot.Channel?.Dispose();
        }
        catch
        {
        }

        try
        {
            slot.Connection?.Dispose();
        }
        catch
        {
        }

        slot.Channel = null;
        slot.Connection = null;
    }

    private sealed class PublisherSlot
    {
        public object Gate { get; } = new();
        public IConnection? Connection { get; set; }
        public IModel? Channel { get; set; }
    }
}
