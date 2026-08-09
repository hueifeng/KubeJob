using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KubeJob.Transport.RabbitMQ;

public sealed class RabbitMqWorkerNotificationService : BackgroundService
{
    private readonly RabbitMqNotificationOptions _rabbitMq;
    private readonly KubeJobWorkerOptions _worker;
    private readonly IWorkerClaimTriggerSource _claimTrigger;
    private readonly ILogger<RabbitMqWorkerNotificationService> _logger;

    public RabbitMqWorkerNotificationService(
        IOptions<RabbitMqNotificationOptions> rabbitMq,
        IOptions<KubeJobWorkerOptions> worker,
        IWorkerClaimTriggerSource claimTrigger,
        ILogger<RabbitMqWorkerNotificationService> logger)
    {
        _rabbitMq = rabbitMq.Value;
        _worker = worker.Value;
        _claimTrigger = claimTrigger;
        _logger = logger;
        _rabbitMq.Validate();
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
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RabbitMQ KubeJob notification listener disconnected");
                await Task.Delay(_rabbitMq.ReconnectDelay, stoppingToken);
            }
        }
    }

    private async Task ConsumeUntilStoppedAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_rabbitMq.ConnectionString, UriKind.Absolute),
            DispatchConsumersAsync = true,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection($"KubeJob.Worker.{_worker.WorkerId}");
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(
            exchange: _rabbitMq.ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);

        channel.BasicQos(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false);

        var channelGate = new object();
        foreach (var logicalQueue in _worker.Queues)
        {
            var consumerQueue = _rabbitMq.GetConsumerQueueName(logicalQueue);
            channel.QueueDeclare(
                queue: consumerQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);
            channel.QueueBind(
                queue: consumerQueue,
                exchange: _rabbitMq.ExchangeName,
                routingKey: logicalQueue,
                arguments: null);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += (_, delivery) =>
            {
                _claimTrigger.Pulse();
                lock (channelGate)
                {
                    channel.BasicAck(delivery.DeliveryTag, multiple: false);
                }
                return Task.CompletedTask;
            };

            channel.BasicConsume(
                queue: consumerQueue,
                autoAck: false,
                consumer: consumer);
        }

        _logger.LogInformation(
            "RabbitMQ KubeJob notifications active for worker {WorkerId}, group {ConsumerGroup}, and queues {Queues}",
            _worker.WorkerId,
            _rabbitMq.ConsumerGroup,
            string.Join(",", _worker.Queues));

        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }
}
