using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Declares the Direct Dispatch topology for the configured consumer group
/// on startup. Logical queues share one physical quorum execution queue and
/// one shared TTL retry queue by default; the logical queue remains the
/// routing key and envelope field. Poison messages route to the shared group
/// DLQ. Cancellation is
/// handled through a separate per-group fanout exchange; each worker declares
/// its own exclusive auto-delete cancel queue so every live worker receives a copy.
///
/// Topology is idempotent: re-declaring with matching arguments is a no-op
/// on the broker, so the host service can re-run safely on every boot.
/// </summary>
public sealed class RabbitMqDispatchTopology : IHostedService
{
    private readonly RabbitMqExecutionOptions _options;
    private readonly KubeJob.Worker.Options.KubeJobWorkerOptions _worker;
    private readonly ILogger<RabbitMqDispatchTopology> _logger;

    public RabbitMqDispatchTopology(
        IOptions<RabbitMqExecutionOptions> options,
        IOptions<KubeJob.Worker.Options.KubeJobWorkerOptions> worker,
        ILogger<RabbitMqDispatchTopology> logger)
    {
        _options = options.Value;
        _worker = worker.Value;
        _logger = logger;
        _options.Validate();
        _worker.Validate();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        const int MaxAttempts = 5;
        var attempt = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                DeclareTopologyOnce();
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                attempt++;
                if (attempt >= MaxAttempts)
                {
                    _logger.LogError(
                        exception,
                        "RabbitMQ KubeJob topology declaration failed after {Attempts} attempts; aborting startup",
                        attempt);
                    throw;
                }

                _logger.LogWarning(
                    exception,
                    "RabbitMQ KubeJob topology declaration failed (attempt {Attempt}/{Max}); retrying in {ReconnectDelay}",
                    attempt,
                    MaxAttempts,
                    _options.ReconnectDelay);
                await Task.Delay(_options.ReconnectDelay, cancellationToken);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    private void DeclareTopologyOnce()
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection("KubeJob.DispatchTopology");
        using var channel = connection.CreateModel();

        DeclareTopology(channel);

        _logger.LogInformation(
            "RabbitMQ KubeJob Direct Dispatch topology declared for group {ConsumerGroup} across queues {Queues}",
            _options.ConsumerGroup,
            string.Join(",", _worker.Queues));
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    internal void DeclareTopology(IModel channel)
    {
        DeclareGroupTopology(channel);
        DeclareRetryTopology(channel);
        if (_options.EnableCancelQueue)
        {
            DeclareCancelTopology(channel);
        }
        foreach (var logicalQueue in _worker.Queues)
        {
            DeclareDispatchQueue(channel, logicalQueue);
        }
    }

    private void DeclareGroupTopology(IModel channel)
    {
        channel.ExchangeDeclare(
            exchange: _options.GetGroupExchangeName(),
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);

        channel.ExchangeDeclare(
            exchange: _options.GetGroupDlxName(),
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null);

        var dlqArguments = new Dictionary<string, object>
        {
            ["x-queue-type"] = "quorum"
        };
        channel.QueueDeclare(
            queue: _options.GetGroupDlqName(),
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: dlqArguments);
        channel.QueueBind(
            queue: _options.GetGroupDlqName(),
            exchange: _options.GetGroupDlxName(),
            routingKey: string.Empty,
            arguments: null);
    }

    private void DeclareRetryTopology(IModel channel)
    {
        channel.ExchangeDeclare(
            exchange: _options.GetRetryExchangeName(),
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);

        var retryArguments = new Dictionary<string, object>
        {
            ["x-queue-type"] = "quorum",
            ["x-message-ttl"] = checked((int)_options.RetryDelay.TotalMilliseconds),
            ["x-dead-letter-exchange"] = _options.GetGroupExchangeName()
        };
        foreach (var logicalQueue in _worker.Queues)
        {
            var retryQueue = _options.GetRetryQueueName(logicalQueue);
            channel.QueueDeclare(
                queue: retryQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: retryArguments);
            channel.QueueBind(
                queue: retryQueue,
                exchange: _options.GetRetryExchangeName(),
                routingKey: logicalQueue,
                arguments: null);
        }
    }

    private void DeclareCancelTopology(IModel channel)
    {
        var exchangeName = _options.GetCancelExchangeName(_options.ConsumerGroup);
        channel.ExchangeDeclare(
            exchange: exchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null);
    }

    private void DeclareDispatchQueue(IModel channel, string logicalQueue)
    {
        var queueName = _options.GetConsumerQueueName(logicalQueue);
        var arguments = new Dictionary<string, object>
        {
            ["x-queue-type"] = "quorum",
            ["x-dead-letter-exchange"] = _options.GetGroupDlxName()
        };
        if (_options.DefaultDeliveryLimit > 0)
        {
            arguments["x-delivery-limit"] = _options.DefaultDeliveryLimit;
        }
        channel.QueueDeclare(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: arguments);
        channel.QueueBind(
            queue: queueName,
            exchange: _options.GetGroupExchangeName(),
            routingKey: logicalQueue,
            arguments: null);

        if (Encoding.UTF8.GetByteCount(logicalQueue) >= 255)
        {
            throw new InvalidOperationException(
                $"RabbitMQ execution routing keys must be shorter than 255 UTF-8 bytes; got '{logicalQueue}'.");
        }
    }
}
