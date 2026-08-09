using System.Text.Json;
using Confluent.Kafka;
using KubeJob.Core.Events;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Transport.Kafka;

/// <summary>
/// Kafka Event Runtime. The shared order.events topic is independently read by
/// the fixed log, data and notify consumer groups. Replicas in one group share
/// partitions and therefore horizontally scale exactly like queue consumers.
/// </summary>
public sealed class KafkaBrokerNativeEventConsumerService : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly KafkaBrokerNativeOptions _options;
    private readonly KubeJobWorkerOptions _worker;
    private readonly BrokerNativeEventProcessor _processor;
    private readonly ILogger<KafkaBrokerNativeEventConsumerService> _logger;
    private readonly SubscriptionGroup[] _groups;
    private readonly IProducer<string, byte[]> _producer;

    public KafkaBrokerNativeEventConsumerService(
        IOptions<KafkaBrokerNativeOptions> options,
        IOptions<KubeJobWorkerOptions> worker,
        BrokerNativeEventProcessor processor,
        IEnumerable<EventSubscriptionDefinition> subscriptions,
        ILogger<KafkaBrokerNativeEventConsumerService> logger)
    {
        _options = options.Value;
        _worker = worker.Value;
        _processor = processor;
        _logger = logger;
        _options.Validate();
        _worker.ValidateEventWorker();
        _groups = subscriptions
            .GroupBy(subscription => KafkaBrokerNativeOptions.GetCapability(subscription.Subscription))
            .Select(group => CreateGroup(group.Key, group))
            .ToArray();
        if (_groups.Length == 0)
        {
            throw new InvalidOperationException("Kafka Event consumer requires at least one AddKubeJobEventHandler subscription.");
        }

        _producer = new ProducerBuilder<string, byte[]>(KafkaClientOptions.CreateProducerConfig(_options)).Build();
    }

    public override void Dispose()
    {
        _producer.Dispose();
        base.Dispose();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.WhenAll(_groups.Select(group => RunGroupUntilStoppedAsync(group, stoppingToken)));
    }

    private async Task RunGroupUntilStoppedAsync(SubscriptionGroup group, CancellationToken stoppingToken)
    {
        var topics = new[]
        {
            _options.EventTopic,
            _options.GetEventRetryTopic(group.Capability),
            _options.GetEventDeadLetterTopic(group.Capability)
        };
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await KafkaTopologyValidator.EnsureAsync(_options, topics, stoppingToken);
                await ConsumeGroupAsync(group, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Kafka Event group {Capability} disconnected for worker {WorkerId}; reconnecting", group.Capability, _worker.WorkerId);
                await Task.Delay(_options.ReconnectDelayMilliseconds, stoppingToken);
            }
        }
    }

    private async Task ConsumeGroupAsync(SubscriptionGroup group, CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(
                KafkaClientOptions.CreateConsumerConfig(_options, _options.GetEventConsumerGroup(group.Capability)))
            .Build();
        consumer.Subscribe([_options.EventTopic, _options.GetEventRetryTopic(group.Capability)]);
        var dispatcher = new KafkaPartitionDispatcher<DeliveryResult>(_worker.MaxConcurrentJobs);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await dispatcher.DrainCompletedAsync(consumer, CommitAsync, Recover);
                var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (record is null || record.IsPartitionEOF)
                {
                    continue;
                }

                if (!dispatcher.TryDispatch(record, (delivery, token) => ProcessAsync(group, delivery, token), stoppingToken))
                {
                    throw new InvalidOperationException($"Kafka partition '{record.TopicPartition}' was consumed while paused.");
                }

                consumer.Pause([record.TopicPartition]);
            }
        }
        finally
        {
            try { await dispatcher.DrainAsync(); } catch (OperationCanceledException) { }
            consumer.Close();
        }

        Task CommitAsync(ConsumeResult<string, byte[]> record, DeliveryResult result)
        {
            consumer.Commit(record);
            return Task.CompletedTask;
        }

        void Recover(ConsumeResult<string, byte[]> record, Exception exception)
        {
            _logger.LogError(exception, "Kafka Event transport failure at {TopicPartitionOffset}; seeking for redelivery", record.TopicPartitionOffset);
            consumer.Seek(record.TopicPartitionOffset);
        }
    }

    private async Task<DeliveryResult> ProcessAsync(
        SubscriptionGroup group,
        ConsumeResult<string, byte[]> record,
        CancellationToken stoppingToken)
    {
        await WaitForRetryDueAsync(record.Message.Headers, stoppingToken);
        BrokerNativeEventMessage message;
        try
        {
            message = JsonSerializer.Deserialize<BrokerNativeEventMessage>(record.Message.Value, SerializerOptions)
                ?? throw new JsonException("BrokerNative event message was empty.");
            message = message with { RetryPolicy = message.RetryPolicy ?? new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5)) };
            message.Validate();
        }
        catch (Exception exception) when (exception is JsonException or InvalidOperationException or ArgumentException)
        {
            _logger.LogWarning(exception, "Kafka Event delivery {TopicPartitionOffset} is malformed; sending to {Capability} DLQ", record.TopicPartitionOffset, group.Capability);
            await PublishAsync(record, _options.GetEventDeadLetterTopic(group.Capability), stoppingToken);
            return DeliveryResult.DeadLetter;
        }

        if (!group.ByEventKey.TryGetValue(EventKey(message.Topic, message.RoutingKey), out var subscription))
        {
            // A shared topic replaces exchange bindings. An event that does not
            // belong to this capability is intentionally ignored and committed;
            // it remains independently available to the other capability groups.
            return DeliveryResult.Ignored;
        }

        var result = await _processor.ProcessAsync(message, subscription, CancellationToken.None, stoppingToken);
        if (result.Disposition == BrokerNativeMessageDisposition.Ack)
        {
            return DeliveryResult.Ack;
        }

        if (result.Disposition == BrokerNativeMessageDisposition.Retry && result.RetryMessage is not null)
        {
            await PublishAsync(
                record,
                _options.GetEventRetryTopic(group.Capability),
                stoppingToken,
                JsonSerializer.SerializeToUtf8Bytes(result.RetryMessage, SerializerOptions),
                DateTimeOffset.UtcNow + _options.GetRetryDelay(result.RetryMessage.RetryPolicy, message.Attempt));
            return DeliveryResult.Retry;
        }

        await PublishAsync(record, _options.GetEventDeadLetterTopic(group.Capability), stoppingToken);
        return DeliveryResult.DeadLetter;
    }

    private async Task PublishAsync(
        ConsumeResult<string, byte[]> record,
        string destination,
        CancellationToken cancellationToken,
        byte[]? body = null,
        DateTimeOffset? notBefore = null)
    {
        var headers = notBefore is null
            ? record.Message.Headers
            : KafkaMessageHeaders.CopyWithNotBefore(record.Message.Headers, notBefore.Value);
        await _producer.ProduceAsync(destination, new Message<string, byte[]>
        {
            Key = record.Message.Key,
            Value = body ?? record.Message.Value,
            Headers = headers
        }, cancellationToken);
    }

    private static async Task WaitForRetryDueAsync(Headers? headers, CancellationToken cancellationToken)
    {
        var notBefore = KafkaMessageHeaders.GetNotBefore(headers);
        if (notBefore is { } due && due > DateTimeOffset.UtcNow)
        {
            await Task.Delay(due - DateTimeOffset.UtcNow, cancellationToken);
        }
    }

    private static SubscriptionGroup CreateGroup(string capability, IEnumerable<EventSubscriptionDefinition> definitions)
    {
        var byEventKey = new Dictionary<string, EventSubscriptionDefinition>(StringComparer.Ordinal);
        foreach (var definition in definitions)
        {
            var key = EventKey(definition.Topic, definition.RoutingKey);
            if (!byEventKey.TryAdd(key, definition))
            {
                throw new InvalidOperationException(
                    $"Kafka capability '{capability}' has multiple handlers for event '{definition.Topic}/{definition.RoutingKey}'.");
            }
        }

        return new SubscriptionGroup(capability, byEventKey);
    }

    private static string EventKey(string topic, string routingKey) => $"{topic}\n{routingKey}";

    private enum DeliveryResult { Ack, Retry, DeadLetter, Ignored }

    private sealed record SubscriptionGroup(
        string Capability,
        IReadOnlyDictionary<string, EventSubscriptionDefinition> ByEventKey);
}
