using System.Text;
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
    private readonly IWorkerRuntimeClient _runtimeClient;
    private readonly RabbitMqDispatchTopology _topology;
    private readonly ILogger<RabbitMqExecutionConsumerService> _logger;

    public RabbitMqExecutionConsumerService(
        IOptions<RabbitMqExecutionOptions> options,
        IOptions<KubeJobWorkerOptions> worker,
        WorkerRuntimeService runtime,
        IWorkerRuntimeClient runtimeClient,
        RabbitMqDispatchTopology topology,
        ILogger<RabbitMqExecutionConsumerService> logger)
    {
        _options = options.Value;
        _worker = worker.Value;
        _runtime = runtime;
        _runtimeClient = runtimeClient;
        _topology = topology;
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
        using var connectionLifetime = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        channel.BasicQos(0, _options.PrefetchCount, global: false);
        channel.ConfirmSelect();

        var channelGate = new object();
        _topology.DeclareTopology(channel);
        var consumerQueues = _worker.Queues
            .Select(_options.GetConsumerQueueName)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        foreach (var consumerQueue in consumerQueues)
        {
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += (_, delivery) => ProcessDeliveryAsync(
                channel,
                channelGate,
                delivery,
                connectionLifetime.Token);
            channel.BasicConsume(
                queue: consumerQueue,
                autoAck: false,
                consumer: consumer);
        }

        if (_options.EnableCancelQueue)
        {
            var cancelQueue = _options.GetCancelQueueName(
                _options.ConsumerGroup,
                $"{_worker.WorkerId}.{_runtime.SessionId}");
            channel.QueueDeclare(
                queue: cancelQueue,
                durable: false,
                exclusive: true,
                autoDelete: true,
                arguments: null);
            channel.QueueBind(
                queue: cancelQueue,
                exchange: _options.GetCancelExchangeName(_options.ConsumerGroup),
                routingKey: string.Empty,
                arguments: null);
            var cancelConsumer = new AsyncEventingBasicConsumer(channel);
            cancelConsumer.Received += (_, delivery) => ProcessCancelDeliveryAsync(
                channel,
                channelGate,
                delivery);
            channel.BasicConsume(
                queue: cancelQueue,
                autoAck: false,
                consumer: cancelConsumer);
        }

        _logger.LogInformation(
            "RabbitMQ KubeJob execution consumer active for worker {WorkerId}, group {ConsumerGroup}, and queues {Queues}",
            _worker.WorkerId,
            _options.ConsumerGroup,
            string.Join(",", _worker.Queues));

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

    private Task ProcessCancelDeliveryAsync(
        IModel channel,
        object channelGate,
        BasicDeliverEventArgs delivery)
    {
        try
        {
            var cancel = JsonSerializer.Deserialize<CancelEnvelope>(
                delivery.Body.Span,
                SerializerOptions)
                ?? throw new JsonException("RabbitMQ cancel envelope was empty.");
            if (string.IsNullOrWhiteSpace(cancel.RunId))
            {
                throw new JsonException("RabbitMQ cancel envelope did not contain a runId.");
            }

            _runtime.TryCancelRun(cancel.RunId);
            Ack(channel, channelGate, delivery.DeliveryTag);
            _logger.LogDebug(
                "ACKed RabbitMQ cancel signal for Run {RunId}",
                cancel.RunId);
        }
        catch (JsonException exception)
        {
            Reject(channel, channelGate, delivery.DeliveryTag, exception.Message);
            _logger.LogWarning(
                exception,
                "Rejected malformed RabbitMQ cancel envelope {DeliveryTag}",
                delivery.DeliveryTag);
        }
        catch (Exception exception)
        {
            Nack(channel, channelGate, delivery.DeliveryTag, exception.Message);
            _logger.LogError(
                exception,
                "Transient failure processing RabbitMQ cancel envelope {DeliveryTag}; requeued",
                delivery.DeliveryTag);
        }

        return Task.CompletedTask;
    }

    private async Task ProcessDeliveryAsync(
        IModel channel,
        object channelGate,
        BasicDeliverEventArgs delivery,
        CancellationToken stoppingToken)
    {
        ExecutionEnvelope? envelope = null;
        try
        {
            envelope = JsonSerializer.Deserialize<ExecutionEnvelope>(
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
                    await RepublishOrReconcileAsync(
                        channel,
                        channelGate,
                        delivery,
                        envelope,
                        "worker_retry",
                        stoppingToken);
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
            _logger.LogDebug(
                "RabbitMQ execution delivery {DeliveryTag} was interrupted by connection or host shutdown; broker will requeue the unacked delivery",
                delivery.DeliveryTag);
        }
        catch (Exception exception)
        {
            if (envelope is not null)
            {
                await RepublishOrReconcileAsync(
                    channel,
                    channelGate,
                    delivery,
                    envelope,
                    exception.Message,
                    stoppingToken);
            }
            else
            {
                Nack(channel, channelGate, delivery.DeliveryTag, exception.Message);
            }
            _logger.LogError(
                exception,
                "Transient failure processing RabbitMQ execution envelope {DeliveryTag}; sent to retry queue when possible",
                delivery.DeliveryTag);
        }
    }

    private async Task RepublishOrReconcileAsync(
        IModel channel,
        object gate,
        BasicDeliverEventArgs delivery,
        ExecutionEnvelope envelope,
        string reason,
        CancellationToken cancellationToken)
    {
        if (GetBrokerRetryCount(delivery.BasicProperties) >= _options.MaxBrokerRetryAttempts)
        {
            try
            {
                var scheduled = await _runtimeClient.RequeueExecutionAsync(
                    new RequeueExecutionRequest(
                        envelope.RunId,
                        DateTimeOffset.UtcNow + _options.BrokerRetryReconciliationDelay),
                    cancellationToken);
                Ack(channel, gate, delivery.DeliveryTag);
                _logger.LogWarning(
                    "ACKed RabbitMQ execution envelope {EventId} after broker retry budget; durable reconciliation scheduled={Scheduled} for Run {RunId}",
                    envelope.EventId,
                    scheduled,
                    envelope.RunId);
            }
            catch (Exception reconciliationException)
            {
                // Reconciliation failed too: drop into the DLQ via requeue=false
                // rather than NACK-ing back to the head of the queue, which
                // would loop indefinitely.
                Reject(channel, gate, delivery.DeliveryTag,
                    $"{reason}; durable reconciliation failed: {reconciliationException.Message}");
            }

            return;
        }

        try
        {
            RepublishForRetry(channel, gate, delivery, envelope.Queue);
        }
        catch (Exception retryException)
        {
            // Same NACK-loop risk: prefer reject (no requeue) so the broker
            // routes the envelope through its DLX. The durable outbox still
            // owns correctness, so we accept a DLQ entry as a poison-pill
            // signal rather than spinning forever.
            Reject(channel, gate, delivery.DeliveryTag,
                $"{reason}; retry publication failed: {retryException.Message}");
        }
    }

    private void RepublishForRetry(
        IModel channel,
        object gate,
        BasicDeliverEventArgs delivery,
        string logicalQueue)
    {
        lock (gate)
        {
            BasicReturnEventArgs? returned = null;
            EventHandler<BasicReturnEventArgs> returnHandler = (_, args) => returned = args;
            channel.BasicReturn += returnHandler;
            try
            {
                var retryCount = GetBrokerRetryCount(delivery.BasicProperties) + 1;
                var properties = delivery.BasicProperties;
                var headers = properties.Headers is null
                    ? new Dictionary<string, object>()
                    : new Dictionary<string, object>(properties.Headers);
                headers[BrokerRetryCountHeader] = retryCount;
                properties.Headers = headers;

                channel.BasicPublish(
                    exchange: _options.GetRetryExchangeName(),
                    routingKey: logicalQueue,
                    mandatory: true,
                    basicProperties: delivery.BasicProperties,
                    body: delivery.Body.ToArray());
                if (!channel.WaitForConfirms(_options.PublisherConfirmTimeout))
                {
                    throw new IOException(
                        $"RabbitMQ did not confirm retry publication for delivery {delivery.DeliveryTag}.");
                }

                if (returned is not null)
                {
                    throw new IOException(
                        $"RabbitMQ could not route retry delivery {delivery.DeliveryTag} for queue '{logicalQueue}'.");
                }

                channel.BasicAck(delivery.DeliveryTag, multiple: false);
            }
            finally
            {
                channel.BasicReturn -= returnHandler;
            }
        }
    }

    private const string BrokerRetryCountHeader = "x-kubejob-broker-retry-count";

    private static int GetBrokerRetryCount(IBasicProperties properties)
    {
        if (properties.Headers is null
            || !properties.Headers.TryGetValue(BrokerRetryCountHeader, out var value))
        {
            return 0;
        }

        return value switch
        {
            byte[] bytes when int.TryParse(Encoding.UTF8.GetString(bytes), out var parsedBytes) => parsedBytes,
            int integer => integer,
            long longValue when longValue is >= 0 and <= int.MaxValue => (int)longValue,
            string text when int.TryParse(text, out var parsedString) => parsedString,
            _ => 0
        };
    }

    private sealed record CancelEnvelope(string RunId);

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
