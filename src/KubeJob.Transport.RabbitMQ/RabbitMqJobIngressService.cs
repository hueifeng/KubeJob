using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// RabbitMQ business-message Adapter. It accepts a message only after the
/// control plane has durably accepted the logical Run, then ACKs it. Invalid
/// messages are rejected for dead-letter handling; transient failures requeue.
/// </summary>
public sealed class RabbitMqJobIngressService : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly RabbitMqJobIngressOptions _options;
    private readonly IJobMessageIngress _ingress;
    private readonly ILogger<RabbitMqJobIngressService> _logger;
    private readonly object _channelGate = new();

    public RabbitMqJobIngressService(
        IOptions<RabbitMqJobIngressOptions> options,
        IJobMessageIngress ingress,
        ILogger<RabbitMqJobIngressService> logger)
    {
        _options = options.Value;
        _ingress = ingress;
        _logger = logger;
        _options.Validate();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeUntilStoppedAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "RabbitMQ KubeJob business ingress disconnected from queue {Queue}",
                    _options.QueueName);
                await Task.Delay(_options.ReconnectDelay, stoppingToken);
            }
        }
    }

    private async Task ConsumeUntilStoppedAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            DispatchConsumersAsync = true,
            ConsumerDispatchConcurrency = _options.ConsumerDispatchConcurrency == 0
                ? Math.Min(_options.PrefetchCount, (ushort)_options.SubmissionBatchSize)
                : _options.ConsumerDispatchConcurrency,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection("KubeJob.RabbitMq.Ingress");
        using var channel = connection.CreateModel();
        using var connectionLifetime = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        await using var batcher = new JobIngressMicroBatcher(
            _ingress,
            _options.SubmissionBatchSize,
            _options.SubmissionBatchWait);
        channel.ExchangeDeclare(
            exchange: _options.ExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            arguments: null);
        if (!string.IsNullOrWhiteSpace(_options.DeadLetterExchangeName))
        {
            channel.ExchangeDeclare(
                exchange: _options.DeadLetterExchangeName,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                arguments: null);
        }
        var queueArguments = CreateQueueArguments();
        channel.QueueDeclare(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArguments);
        channel.QueueBind(
            queue: _options.QueueName,
            exchange: _options.ExchangeName,
            routingKey: _options.RoutingKey,
            arguments: null);
        channel.BasicQos(0, _options.PrefetchCount, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, delivery) =>
        {
            await ProcessDeliveryAsync(channel, batcher, delivery, connectionLifetime.Token);
        };
        channel.BasicConsume(
            queue: _options.QueueName,
            autoAck: false,
            consumer: consumer);

        _logger.LogInformation(
            "RabbitMQ KubeJob business ingress active on exchange {Exchange}, queue {Queue}, source {Source}",
            _options.ExchangeName,
            _options.QueueName,
            _options.Source);

        var disconnected = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        connection.ConnectionShutdown += (_, _) =>
        {
            connectionLifetime.Cancel();
            disconnected.TrySetResult();
        };
        await Task.WhenAny(
            disconnected.Task,
            Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken));
        connectionLifetime.Cancel();
        stoppingToken.ThrowIfCancellationRequested();
    }

    private async Task ProcessDeliveryAsync(
        IModel channel,
        JobIngressMicroBatcher batcher,
        BasicDeliverEventArgs delivery,
        CancellationToken stoppingToken)
    {
        try
        {
            var envelope = JsonSerializer.Deserialize<RabbitMqJobIngressEnvelope>(
                delivery.Body.Span,
                SerializerOptions)
                ?? throw new JsonException("RabbitMQ ingress body was empty.");
            var messageId = string.IsNullOrWhiteSpace(delivery.BasicProperties.MessageId)
                ? envelope.MessageId
                : delivery.BasicProperties.MessageId;
            EnsureIngressIdentity(_options.Source, messageId);
            var request = new EnqueueJobRequest(
                envelope.JobKey,
                envelope.PayloadJson,
                envelope.Queue,
                envelope.Priority,
                envelope.NotBefore,
                IdempotencyKey: null,
                envelope.ConcurrencyKey,
                envelope.MaxAttempts,
                envelope.TimeoutSeconds,
                RetryPolicy: null);

            var result = await batcher.SubmitAsync(
                new JobIngressMessage(_options.Source, messageId, request),
                stoppingToken);
            Ack(channel, delivery.DeliveryTag);
            _logger.LogDebug(
                "Accepted RabbitMQ KubeJob ingress message {MessageId} as Run {RunId}; existing={Existing}",
                messageId,
                result.JobId,
                result.Existing);
        }
        catch (JsonException exception)
        {
            Reject(channel, delivery.DeliveryTag);
            _logger.LogWarning(
                exception,
                "Rejected malformed RabbitMQ KubeJob ingress message {DeliveryTag}",
                delivery.DeliveryTag);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogDebug(
                "RabbitMQ ingress delivery {DeliveryTag} was interrupted by connection or host shutdown; broker will requeue the unacked delivery",
                delivery.DeliveryTag);
        }
        catch (ControlPlaneValidationException exception)
        {
            Reject(channel, delivery.DeliveryTag);
            _logger.LogWarning(
                exception,
                "Rejected invalid RabbitMQ KubeJob ingress message {DeliveryTag} with code {Code}",
                delivery.DeliveryTag,
                exception.Code);
        }
        catch (IdempotencyConflictException exception)
        {
            Reject(channel, delivery.DeliveryTag);
            _logger.LogWarning(
                exception,
                "Rejected conflicting RabbitMQ KubeJob ingress message {DeliveryTag}",
                delivery.DeliveryTag);
        }
        catch (Exception exception)
        {
            Nack(channel, delivery.DeliveryTag);
            _logger.LogError(
                exception,
                "Transient failure processing RabbitMQ KubeJob ingress message {DeliveryTag}; requeued",
                delivery.DeliveryTag);
        }
    }

    private void Ack(IModel channel, ulong deliveryTag)
    {
        lock (_channelGate)
        {
            channel.BasicAck(deliveryTag, multiple: false);
        }
    }

    private void Reject(IModel channel, ulong deliveryTag)
    {
        lock (_channelGate)
        {
            channel.BasicReject(deliveryTag, requeue: false);
        }
    }

    private void Nack(IModel channel, ulong deliveryTag)
    {
        lock (_channelGate)
        {
            channel.BasicNack(deliveryTag, multiple: false, requeue: true);
        }
    }

    private IDictionary<string, object>? CreateQueueArguments()
    {
        if (string.IsNullOrWhiteSpace(_options.DeadLetterExchangeName))
        {
            return null;
        }

        return new Dictionary<string, object>
        {
            ["x-dead-letter-exchange"] = _options.DeadLetterExchangeName,
            ["x-dead-letter-routing-key"] = _options.DeadLetterRoutingKey ?? string.Empty
        };
    }

    private static void EnsureIngressIdentity(string? source, string? messageId)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ControlPlaneValidationException(
                "ingress_source_required",
                "RabbitMQ ingress Source must be configured to derive a stable idempotency key.");
        }

        if (string.IsNullOrWhiteSpace(messageId))
        {
            throw new ControlPlaneValidationException(
                "ingress_message_id_required",
                "RabbitMQ ingress MessageId is required. Configure the AMQP MessageId property or include a non-empty MessageId field in the envelope body.");
        }
    }
}
