using System.Diagnostics;
using System.Text.Json;
using KubeJob.Core.Events;
using KubeJob.Core.Transport;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Transport-neutral event publisher. Topics are logical business streams;
/// deployment configuration selects the physical broker adapter.
/// </summary>
public sealed class DefaultEventBus : IEventBus
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly IMessageTransportRegistry _transports;
    private readonly IOptionsMonitor<EventRuntimeOptions> _options;

    public DefaultEventBus(
        IMessageTransportRegistry transports,
        IOptionsMonitor<EventRuntimeOptions> options)
    {
        _transports = transports;
        _options = options;
    }

    public ValueTask<EventHandle> PublishAsync<TEvent>(
        EventKey<TEvent> eventKey,
        TEvent @event,
        CancellationToken cancellationToken = default)
        => PublishAsync(
            eventKey,
            @event,
            new EventPublishOptions(),
            cancellationToken);

    public async ValueTask<EventHandle> PublishAsync<TEvent>(
        EventKey<TEvent> eventKey,
        TEvent @event,
        EventPublishOptions options,
        CancellationToken cancellationToken = default)
    {
        if (eventKey.IsEmpty)
        {
            throw new ArgumentException("The event key must be initialized.", nameof(eventKey));
        }

        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        var topic = eventKey.Topic.Trim();
        var routingKey = eventKey.RoutingKey.Trim();
        var transportId = _options.CurrentValue.ResolveTransportId(topic);
        var publisher = _transports.GetRequiredPublisher(transportId);

        if (!string.IsNullOrWhiteSpace(options.PartitionKey)
            && !publisher.Capabilities.HasFlag(MessageTransportCapabilities.Partitioning))
        {
            throw new NotSupportedException(
                $"Transport '{transportId}' does not advertise native partitioning for topic '{topic}'.");
        }

        var eventId = Guid.NewGuid().ToString("N");
        var activity = Activity.Current;
        var envelope = new BrokerNativeEventMessage
        {
            EventId = eventId,
            Topic = topic,
            RoutingKey = routingKey,
            PayloadJson = JsonSerializer.Serialize(@event, SerializerOptions),
            OccurredAt = DateTimeOffset.UtcNow,
            Attempt = 1,
            MaxAttempts = options.MaxAttempts,
            TimeoutSeconds = checked((int)Math.Ceiling(options.Timeout.TotalSeconds)),
            RetryPolicy = options.RetryPolicy,
            PartitionKey = options.PartitionKey,
            IdempotencyKey = options.IdempotencyKey,
            CorrelationId = activity?.TraceId.ToString(),
            TraceParent = activity?.Id,
            Headers = options.Headers
        };
        envelope.Validate();

        var body = JsonSerializer.SerializeToUtf8Bytes(envelope, SerializerOptions);
        await publisher.PublishAsync(
            new TransportPublishRequest(
                TransportMessageKind.Event,
                topic,
                new TransportMessage(
                    eventId,
                    "kubejob.broker-native.event.v1",
                    body,
                    options.Headers,
                    envelope.CorrelationId,
                    options.PartitionKey),
                RoutingKey: routingKey),
            cancellationToken);

        return new EventHandle(eventId);
    }
}
