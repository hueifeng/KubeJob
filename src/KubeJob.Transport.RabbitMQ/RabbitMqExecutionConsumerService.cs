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
/// Consumes durable Execution Envelopes and delegates the whole execution
/// lifecycle to WorkerRuntimeService. ACK is emitted only after the worker has
/// durably completed or explicitly classified the Run as terminal/rejected.
/// </summary>
public sealed class RabbitMqExecutionConsumerService : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly RabbitMqExecutionOptions _options;
    private readonly KubeJobWorkerOptions _worker;
    private readonly WorkerRuntimeService _runtime;
    private readonly ILogger<RabbitMqExecutionConsumerService> _logger;

    public RabbitMqExecutionConsumerService(
        IOptions<RabbitMqExecutionOptions> options,
        IOptions<KubeJobWorkerOptions> worker,
        WorkerRuntimeService runtime,
        ILogger<RabbitMqExecutionConsumerService> logger)
    {
        _options = options.Value;
        _worker = worker.Value;
        _runtime = runtime;
        _logger = logger;
        _options.Validate();
        _worker.Validate();
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
                    "RabbitMQ KubeJob execution consumer disconnected for worker {WorkerId}",
                    _worker.WorkerId);
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
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection($"KubeJob.Execution.{_worker.WorkerId}");
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(
            exchange: _options.ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);
        channel.BasicQos(0, _options.PrefetchCount, global: false);

        var channelGate = new object();
        foreach (var logicalQueue in _worker.Queues)
        {
            var consumerQueue = _options.GetConsumerQueueName(logicalQueue);
            channel.QueueDeclare(
                queue: consumerQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);
            channel.QueueBind(
                queue: consumerQueue,
                exchange: _options.ExchangeName,
                routingKey: logicalQueue,
                arguments: null);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += (_, delivery) => ProcessDeliveryAsync(
                channel,
                channelGate,
                delivery,
                stoppingToken);
            channel.BasicConsume(
                queue: consumerQueue,
                autoAck: false,
                consumer: consumer);
        }

        _logger.LogInformation(
            "RabbitMQ KubeJob execution consumer active for worker {WorkerId}, group {ConsumerGroup}, and queues {Queues}",
            _worker.WorkerId,
            _options.ConsumerGroup,
            string.Join(",", _worker.Queues));

        var disconnected = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        connection.ConnectionShutdown += (_, _) => disconnected.TrySetResult();
        await Task.WhenAny(
            disconnected.Task,
            Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken));
        stoppingToken.ThrowIfCancellationRequested();
    }

    private async Task ProcessDeliveryAsync(
        IModel channel,
        object channelGate,
        BasicDeliverEventArgs delivery,
        CancellationToken stoppingToken)
    {
        try
        {
            var envelope = JsonSerializer.Deserialize<ExecutionEnvelope>(
                delivery.Body.Span,
                SerializerOptions)
                ?? throw new JsonException("RabbitMQ execution envelope was empty.");

            var result = await _runtime.ProcessExecutionEnvelopeAsync(
                envelope,
                stoppingToken);
            switch (result.Status)
            {
                case ExecutionEnvelopeProcessingStatus.Completed:
                    Ack(channel, channelGate, delivery.DeliveryTag);
                    _logger.LogDebug(
                        "ACKed RabbitMQ execution envelope {EventId} for Run {RunId}",
                        envelope.EventId,
                        envelope.RunId);
                    break;
                case ExecutionEnvelopeProcessingStatus.Reject:
                    Reject(channel, channelGate, delivery.DeliveryTag, result.Reason);
                    break;
                case ExecutionEnvelopeProcessingStatus.Retry:
                    await Task.Delay(_options.RetryDelay, stoppingToken);
                    Nack(channel, channelGate, delivery.DeliveryTag, result.Reason);
                    break;
            }
        }
        catch (JsonException exception)
        {
            Reject(channel, channelGate, delivery.DeliveryTag, exception.Message);
            _logger.LogWarning(
                exception,
                "Rejected malformed RabbitMQ execution envelope {DeliveryTag}",
                delivery.DeliveryTag);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            Nack(channel, channelGate, delivery.DeliveryTag, "consumer_stopping");
        }
        catch (Exception exception)
        {
            Nack(channel, channelGate, delivery.DeliveryTag, exception.Message);
            _logger.LogError(
                exception,
                "Transient failure processing RabbitMQ execution envelope {DeliveryTag}; requeued",
                delivery.DeliveryTag);
        }
    }

    private void Ack(IModel channel, object gate, ulong deliveryTag)
    {
        lock (gate)
        {
            channel.BasicAck(deliveryTag, multiple: false);
        }
    }

    private void Reject(IModel channel, object gate, ulong deliveryTag, string? reason)
    {
        lock (gate)
        {
            channel.BasicReject(deliveryTag, requeue: false);
        }

        _logger.LogWarning(
            "Rejected RabbitMQ execution envelope {DeliveryTag}: {Reason}",
            deliveryTag,
            reason ?? "unspecified");
    }

    private void Nack(IModel channel, object gate, ulong deliveryTag, string? reason)
    {
        lock (gate)
        {
            channel.BasicNack(deliveryTag, multiple: false, requeue: true);
        }

        _logger.LogDebug(
            "Requeued RabbitMQ execution envelope {DeliveryTag}: {Reason}",
            deliveryTag,
            reason ?? "unspecified");
    }
}
