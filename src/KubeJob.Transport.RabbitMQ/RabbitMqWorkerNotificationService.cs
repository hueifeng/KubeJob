using KubeJob.Worker.Options;
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
    private readonly WorkerWakeSignal _signal;
    private readonly ILogger<RabbitMqWorkerNotificationService> _logger;

    public RabbitMqWorkerNotificationService(
        IOptions<RabbitMqNotificationOptions> rabbitMq,
        IOptions<KubeJobWorkerOptions> worker,
        WorkerWakeSignal signal,
        ILogger<RabbitMqWorkerNotificationService> logger)
    {
        _rabbitMq = rabbitMq.Value;
        _worker = worker.Value;
        _signal = signal;
        _logger = logger;
        _rabbitMq.Validate();
        _worker.ValidateV2();
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

        var declared = channel.QueueDeclare(
            queue: string.Empty,
            durable: false,
            exclusive: true,
            autoDelete: true,
            arguments: null);

        foreach (var queue in _worker.Queues.Distinct(StringComparer.Ordinal))
        {
            channel.QueueBind(
                queue: declared.QueueName,
                exchange: _rabbitMq.ExchangeName,
                routingKey: queue,
                arguments: null);
        }

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += (_, _) =>
        {
            _signal.Pulse();
            return Task.CompletedTask;
        };

        channel.BasicConsume(
            queue: declared.QueueName,
            autoAck: true,
            consumer: consumer);

        _logger.LogInformation(
            "RabbitMQ KubeJob notifications active for worker {WorkerId} and queues {Queues}",
            _worker.WorkerId,
            string.Join(",", _worker.Queues));

        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }
}
