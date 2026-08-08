using System.Text.Json;
using KubeJob.Core.Events;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// RabbitMQ Event Runtime. Each (Topic, Subscription) owns one physical queue;
/// all replicas of the same subscription compete for deliveries while distinct
/// subscriptions receive independent copies from the Topic exchange.
/// </summary>
public sealed class RabbitMqBrokerNativeEventConsumerService : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly RabbitMqBrokerNativeOptions _options;
    private readonly KubeJobWorkerOptions _worker;
    private readonly BrokerNativeEventProcessor _processor;
    private readonly ILogger<RabbitMqBrokerNativeEventConsumerService> _logger;
    private readonly SubscriptionGroup[] _groups;
    private readonly SemaphoreSlim _executionSlots;

    public RabbitMqBrokerNativeEventConsumerService(
        IOptions<RabbitMqBrokerNativeOptions> options,
        IOptions<KubeJobWorkerOptions> worker,
        BrokerNativeEventProcessor processor,
        IEnumerable<EventSubscriptionDefinition> subscriptions,
        ILogger<RabbitMqBrokerNativeEventConsumerService> logger)
    {
        _options = options.Value;
        _worker = worker.Value;
        _processor = processor;
        _logger = logger;
        _options.Validate();
        _worker.Validate(requireJobQueues: false);

        _groups = subscriptions
            .GroupBy(
                item => (item.Topic, item.Subscription),
                item => item)
            .Select(group => CreateGroup(group.Key.Topic, group.Key.Subscription, group))
            .ToArray();

        if (_groups.Length == 0)
        {
            throw new InvalidOperationException(
                "RabbitMQ Event consumer requires at least one AddKubeJobEventHandler subscription.");
        }

        _executionSlots = new SemaphoreSlim(
            _worker.MaxConcurrentJobs,
            _worker.MaxConcurrentJobs);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeUntilDisconnectedAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "RabbitMQ Event consumer disconnected for worker {WorkerId}; reconnecting",
                    _worker.WorkerId);
                await Task.Delay(_options.ReconnectDelay, stoppingToken);
            }
        }
    }

    private async Task ConsumeUntilDisconnectedAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            DispatchConsumersAsync = true,
            ConsumerDispatchConcurrency = _options.ConsumerDispatchConcurrency == 0
                ? checked((ushort)Math.Min(ushort.MaxValue, Math.Max(1, _worker.MaxConcurrentJobs)))
                : _options.ConsumerDispatchConcurrency,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection($"KubeJob.Events.{_worker.WorkerId}");
        using var consumeChannel = connection.CreateModel();
        using var publishChannel = connection.CreateModel();
        using var connectionLifetime = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        consumeChannel.BasicQos(0, _options.PrefetchCount, global: false);
        publishChannel.ConfirmSelect();

        var consumeChannelGate = new object();
        var publishChannelGate = new object();
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        void SignalDisconnected()
        {
            connectionLifetime.Cancel();
            disconnected.TrySetResult();
        }

        foreach (var group in _groups)
        {
            var queue = RabbitMqEventTopology.DeclareSubscription(
                consumeChannel,
                _options,
                group.Topic,
                group.Subscription,
                group.ByRoutingKey.Values);

            var expectedGroup = group;
            var consumer = new AsyncEventingBasicConsumer(consumeChannel);
            consumer.ConsumerCancelled += (_, _) =>
            {
                SignalDisconnected();
                return Task.CompletedTask;
            };
            consumer.Received += (_, delivery) => ProcessDeliveryAsync(
                expectedGroup,
                delivery,
                consumeChannel,
                consumeChannelGate,
                publishChannel,
                publishChannelGate,
                connectionLifetime.Token);

            consumeChannel.BasicConsume(
                queue,
                autoAck: false,
                consumer);
        }

        connection.ConnectionShutdown += (_, _) => SignalDisconnected();
        connection.CallbackException += (_, _) => SignalDisconnected();
        consumeChannel.ModelShutdown += (_, _) => SignalDisconnected();

        _logger.LogInformation(
            "RabbitMQ Event consumer active for worker {WorkerId}; subscriptions {Subscriptions}; concurrency {Concurrency}",
            _worker.WorkerId,
            string.Join(",", _groups.Select(group => $"{group.Topic}/{group.Subscription}")),
            _worker.MaxConcurrentJobs);

        await Task.WhenAny(
            disconnected.Task,
            Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken));

        connectionLifetime.Cancel();
        stoppingToken.ThrowIfCancellationRequested();
    }

    private async Task ProcessDeliveryAsync(
        SubscriptionGroup group,
        BasicDeliverEventArgs delivery,
        IModel consumeChannel,
        object consumeChannelGate,
        IModel publishChannel,
        object publishChannelGate,
        CancellationToken stoppingToken)
    {
        try
        {
            await _executionSlots.WaitAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }

        try
        {
            BrokerNativeEventMessage message;
            EventSubscriptionDefinition definition;
            try
            {
                message = JsonSerializer.Deserialize<BrokerNativeEventMessage>(
                    delivery.Body.Span,
                    SerializerOptions)
                    ?? throw new JsonException("BrokerNative event message was empty.");
                message.Validate();

                if (!string.Equals(message.Topic, group.Topic, StringComparison.Ordinal))
                {
                    throw new JsonException(
                        $"Event topic '{message.Topic}' does not match subscription topic '{group.Topic}'.");
                }

                if (!group.ByRoutingKey.TryGetValue(message.RoutingKey, out definition!))
                {
                    throw new JsonException(
                        $"No handler in subscription '{group.Subscription}' accepts routing key '{message.RoutingKey}'.");
                }
            }
            catch (Exception exception) when (
                exception is JsonException or InvalidOperationException or ArgumentException)
            {
                Reject(consumeChannel, consumeChannelGate, delivery.DeliveryTag);
                _logger.LogWarning(
                    exception,
                    "Dead-lettered malformed Event delivery {DeliveryTag} for {Topic}/{Subscription}",
                    delivery.DeliveryTag,
                    group.Topic,
                    group.Subscription);
                return;
            }

            BrokerNativeEventProcessingResult result;
            try
            {
                result = await _processor.ProcessAsync(
                    message,
                    definition,
                    CancellationToken.None,
                    stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            switch (result.Disposition)
            {
                case BrokerNativeMessageDisposition.Ack:
                    Ack(consumeChannel, consumeChannelGate, delivery.DeliveryTag);
                    break;

                case BrokerNativeMessageDisposition.DeadLetter:
                    Reject(consumeChannel, consumeChannelGate, delivery.DeliveryTag);
                    _logger.LogWarning(
                        "Dead-lettered event {EventId} for subscription {Subscription} after attempt {Attempt}: {FailureCode}",
                        message.EventId,
                        group.Subscription,
                        message.Attempt,
                        result.Execution.FailureCode);
                    break;

                case BrokerNativeMessageDisposition.Retry when result.RetryMessage is not null:
                    PublishRetryThenAck(
                        group,
                        delivery,
                        result.RetryMessage,
                        consumeChannel,
                        consumeChannelGate,
                        publishChannel,
                        publishChannelGate);
                    break;

                default:
                    Reject(consumeChannel, consumeChannelGate, delivery.DeliveryTag);
                    _logger.LogError(
                        "Invalid Event disposition for event {EventId}; dead-lettered for subscription {Subscription}",
                        message.EventId,
                        group.Subscription);
                    break;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Connection close causes redelivery.
        }
        catch (Exception exception)
        {
            Nack(consumeChannel, consumeChannelGate, delivery.DeliveryTag);
            _logger.LogError(
                exception,
                "Transient RabbitMQ Event transport failure for delivery {DeliveryTag}; requeued",
                delivery.DeliveryTag);
        }
        finally
        {
            _executionSlots.Release();
        }
    }

    private void PublishRetryThenAck(
        SubscriptionGroup group,
        BasicDeliverEventArgs original,
        BrokerNativeEventMessage retryMessage,
        IModel consumeChannel,
        object consumeChannelGate,
        IModel publishChannel,
        object publishChannelGate)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(retryMessage, SerializerOptions);
        lock (publishChannelGate)
        {
            BasicReturnEventArgs? returned = null;
            EventHandler<BasicReturnEventArgs> returnHandler = (_, args) =>
            {
                if (string.Equals(
                        args.BasicProperties.MessageId,
                        retryMessage.EventId,
                        StringComparison.Ordinal))
                {
                    returned = args;
                }
            };

            publishChannel.BasicReturn += returnHandler;
            try
            {
                var properties = publishChannel.CreateBasicProperties();
                properties.Persistent = true;
                properties.ContentType = "application/json";
                properties.Type = "kubejob.broker-native.event.v1";
                properties.MessageId = retryMessage.EventId;
                properties.CorrelationId = retryMessage.CorrelationId;
                properties.Headers = original.BasicProperties.Headers is null
                    ? new Dictionary<string, object>()
                    : new Dictionary<string, object>(original.BasicProperties.Headers);
                properties.Headers["x-kubejob-attempt"] = retryMessage.Attempt;
                properties.Headers["x-kubejob-subscription"] = group.Subscription;

                publishChannel.BasicPublish(
                    _options.GetEventRetryExchangeName(group.Topic),
                    group.Subscription,
                    mandatory: true,
                    basicProperties: properties,
                    body: body);

                if (!publishChannel.WaitForConfirms(_options.PublisherConfirmTimeout))
                {
                    throw new IOException(
                        $"RabbitMQ did not confirm event retry '{retryMessage.EventId}' " +
                        $"for subscription '{group.Subscription}'.");
                }

                if (returned is not null)
                {
                    throw new IOException(
                        $"RabbitMQ could not route event retry for '{group.Topic}/{group.Subscription}'.");
                }

                Ack(consumeChannel, consumeChannelGate, original.DeliveryTag);
            }
            finally
            {
                publishChannel.BasicReturn -= returnHandler;
            }
        }
    }

    private static SubscriptionGroup CreateGroup(
        string topic,
        string subscription,
        IEnumerable<EventSubscriptionDefinition> definitions)
    {
        var byRoutingKey = new Dictionary<string, EventSubscriptionDefinition>(StringComparer.Ordinal);
        foreach (var definition in definitions)
        {
            if (!byRoutingKey.TryAdd(definition.RoutingKey, definition))
            {
                throw new InvalidOperationException(
                    $"Subscription '{topic}/{subscription}' has multiple handlers for routing key " +
                    $"'{definition.RoutingKey}'. Use separate Subscription names for independent consumers.");
            }
        }

        return new SubscriptionGroup(topic, subscription, byRoutingKey);
    }

    private static void Ack(IModel channel, object gate, ulong deliveryTag)
    {
        lock (gate)
        {
            if (channel.IsOpen)
            {
                channel.BasicAck(deliveryTag, multiple: false);
            }
        }
    }

    private static void Reject(IModel channel, object gate, ulong deliveryTag)
    {
        lock (gate)
        {
            if (channel.IsOpen)
            {
                channel.BasicReject(deliveryTag, requeue: false);
            }
        }
    }

    private static void Nack(IModel channel, object gate, ulong deliveryTag)
    {
        lock (gate)
        {
            if (channel.IsOpen)
            {
                channel.BasicNack(deliveryTag, multiple: false, requeue: true);
            }
        }
    }

    private sealed record SubscriptionGroup(
        string Topic,
        string Subscription,
        IReadOnlyDictionary<string, EventSubscriptionDefinition> ByRoutingKey);
}
