using System.Text.Json;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// RabbitMQ-authoritative BrokerNative consumer. Deliveries execute directly
/// through <see cref="BrokerNativeJobProcessor"/>; no control-plane admission,
/// Run lookup, lease, or completion database write occurs on this path.
/// Multiple worker replicas consume the same physical queue competitively.
/// </summary>
public sealed class RabbitMqBrokerNativeConsumerService : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly RabbitMqBrokerNativeOptions _options;
    private readonly KubeJobWorkerOptions _worker;
    private readonly BrokerNativeJobProcessor _processor;
    private readonly ILogger<RabbitMqBrokerNativeConsumerService> _logger;
    private readonly SemaphoreSlim _executionSlots;

    public RabbitMqBrokerNativeConsumerService(
        IOptions<RabbitMqBrokerNativeOptions> options,
        IOptions<KubeJobWorkerOptions> worker,
        BrokerNativeJobProcessor processor,
        ILogger<RabbitMqBrokerNativeConsumerService> logger)
    {
        _options = options.Value;
        _worker = worker.Value;
        _processor = processor;
        _logger = logger;
        _options.Validate();
        _worker.Validate();
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
                    "RabbitMQ BrokerNative consumer disconnected for worker {WorkerId}; reconnecting",
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

        using var connection = factory.CreateConnection($"KubeJob.BrokerNative.{_worker.WorkerId}");
        using var consumeChannel = connection.CreateModel();
        using var publishChannel = connection.CreateModel();
        using var connectionLifetime = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        consumeChannel.BasicQos(0, _options.PrefetchCount, global: false);
        publishChannel.ConfirmSelect();

        RabbitMqBrokerNativeTopology.Declare(
            consumeChannel,
            _options,
            _worker.Queues);

        var consumeChannelGate = new object();
        var publishChannelGate = new object();
        var disconnected = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        void SignalDisconnected()
        {
            connectionLifetime.Cancel();
            disconnected.TrySetResult();
        }

        foreach (var logicalQueue in _worker.Queues)
        {
            var expectedQueue = logicalQueue;
            var consumer = new AsyncEventingBasicConsumer(consumeChannel);
            consumer.ConsumerCancelled += (_, _) =>
            {
                SignalDisconnected();
                return Task.CompletedTask;
            };
            consumer.Received += (_, delivery) => ProcessDeliveryAsync(
                expectedQueue,
                delivery,
                consumeChannel,
                consumeChannelGate,
                publishChannel,
                publishChannelGate,
                connectionLifetime.Token);

            consumeChannel.BasicConsume(
                queue: _options.GetQueueName(expectedQueue),
                autoAck: false,
                consumer: consumer);
        }

        connection.ConnectionShutdown += (_, _) => SignalDisconnected();
        connection.CallbackException += (_, _) => SignalDisconnected();
        consumeChannel.ModelShutdown += (_, _) => SignalDisconnected();

        _logger.LogInformation(
            "RabbitMQ BrokerNative consumer active for worker {WorkerId}; exchange {Exchange}; queues {Queues}; concurrency {Concurrency}",
            _worker.WorkerId,
            _options.ExchangeName,
            string.Join(",", _worker.Queues),
            _worker.MaxConcurrentJobs);

        await Task.WhenAny(
            disconnected.Task,
            Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken));

        connectionLifetime.Cancel();
        stoppingToken.ThrowIfCancellationRequested();
    }

    private async Task ProcessDeliveryAsync(
        string expectedLogicalQueue,
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
            // Keep the broker delivery unacked; connection shutdown causes
            // RabbitMQ to redeliver it to another live worker.
            return;
        }

        try
        {
            BrokerNativeJobMessage message;
            try
            {
                message = JsonSerializer.Deserialize<BrokerNativeJobMessage>(
                    delivery.Body.Span,
                    SerializerOptions)
                    ?? throw new JsonException("BrokerNative job message was empty.");
                message.Validate();
                if (!string.Equals(message.Queue, expectedLogicalQueue, StringComparison.Ordinal))
                {
                    throw new JsonException(
                        $"BrokerNative message queue '{message.Queue}' does not match delivery queue '{expectedLogicalQueue}'.");
                }
            }
            catch (Exception exception) when (
                exception is JsonException or InvalidOperationException or ArgumentException)
            {
                Reject(consumeChannel, consumeChannelGate, delivery.DeliveryTag);
                _logger.LogWarning(
                    exception,
                    "Dead-lettered malformed BrokerNative delivery {DeliveryTag}",
                    delivery.DeliveryTag);
                return;
            }

            BrokerNativeProcessingResult result;
            try
            {
                result = await _processor.ProcessAsync(
                    message,
                    CancellationToken.None,
                    stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Shutdown is not a job outcome. Leave this delivery unacked.
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
                        "Dead-lettered BrokerNative message {MessageId} ({JobKey}) after attempt {Attempt}: {FailureCode}",
                        message.MessageId,
                        message.JobKey,
                        message.Attempt,
                        result.Execution.FailureCode);
                    break;

                case BrokerNativeMessageDisposition.Retry when result.RetryMessage is not null:
                    PublishRetryThenAck(
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
                        "Invalid BrokerNative disposition for message {MessageId}; dead-lettered",
                        message.MessageId);
                    break;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Unacked delivery is recovered by RabbitMQ on connection close.
        }
        catch (Exception exception)
        {
            // Handler failures are normalized by BrokerNativeJobProcessor. An
            // exception here is transport/infrastructure failure, so preserve
            // at-least-once delivery by requeueing the original message.
            Nack(consumeChannel, consumeChannelGate, delivery.DeliveryTag);
            _logger.LogError(
                exception,
                "Transient BrokerNative transport failure for delivery {DeliveryTag}; requeued",
                delivery.DeliveryTag);
        }
        finally
        {
            _executionSlots.Release();
        }
    }

    private void PublishRetryThenAck(
        BasicDeliverEventArgs original,
        BrokerNativeJobMessage retryMessage,
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
                        retryMessage.MessageId,
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
                properties.Type = "kubejob.broker-native.job";
                properties.MessageId = retryMessage.MessageId;
                properties.CorrelationId = retryMessage.CorrelationId;
                properties.Headers = original.BasicProperties.Headers is null
                    ? new Dictionary<string, object>()
                    : new Dictionary<string, object>(original.BasicProperties.Headers);
                properties.Headers["x-kubejob-attempt"] = retryMessage.Attempt;

                publishChannel.BasicPublish(
                    exchange: _options.GetRetryExchangeName(),
                    routingKey: retryMessage.Queue,
                    mandatory: true,
                    basicProperties: properties,
                    body: body);

                if (!publishChannel.WaitForConfirms(_options.PublisherConfirmTimeout))
                {
                    throw new IOException(
                        $"RabbitMQ did not confirm BrokerNative retry for message '{retryMessage.MessageId}'.");
                }

                if (returned is not null)
                {
                    throw new IOException(
                        $"RabbitMQ could not route BrokerNative retry for queue '{retryMessage.Queue}'.");
                }

                // Only ACK the original after the retry copy is durably
                // accepted by RabbitMQ. This is the key at-least-once handoff.
                Ack(consumeChannel, consumeChannelGate, original.DeliveryTag);
            }
            finally
            {
                publishChannel.BasicReturn -= returnHandler;
            }
        }
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
}
