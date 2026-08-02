using System.Text;
using KubeJob.ControlPlane.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Raised when the broker topology does not match what this deployment
/// declares (a queue or exchange is missing, or an existing queue carries
/// different arguments). The consumer treats this as a configuration error:
/// it fails fast instead of retrying forever against an incompatible broker.
/// </summary>
public sealed class RabbitMqTopologyMismatchException : InvalidOperationException
{
    public RabbitMqTopologyMismatchException(string message)
        : base(message)
    {
    }

    public RabbitMqTopologyMismatchException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Declares the Direct Dispatch topology for the configured consumer group
/// on startup. Each logical queue receives its own physical quorum execution
/// queue and TTL retry queue by default; the logical queue remains the routing
/// key and envelope field. Poison messages route to the shared group DLQ.
/// Cancellation is
/// handled through a separate per-group fanout exchange; each worker declares
/// its own stable auto-delete cancel queue so every live worker receives a copy.
///
/// Topology is idempotent: re-declaring with matching arguments is a no-op
/// on the broker, so the host service can re-run safely on every boot.
/// </summary>
public sealed class RabbitMqTopologyProvisioner : IHostedService
{
    private readonly RabbitMqExecutionOptions _options;
    private readonly KubeJob.Worker.Options.KubeJobWorkerOptions _worker;
    private readonly QueueCatalog _queueCatalog;
    private readonly ILogger<RabbitMqTopologyProvisioner> _logger;

    public RabbitMqTopologyProvisioner(
        IOptions<RabbitMqExecutionOptions> options,
        IOptions<KubeJob.Worker.Options.KubeJobWorkerOptions> worker,
        QueueCatalog queueCatalog,
        ILogger<RabbitMqTopologyProvisioner> logger)
    {
        _options = options.Value;
        _worker = worker.Value;
        _queueCatalog = queueCatalog;
        _logger = logger;
        _options.Validate();
        _worker.Validate();
        ValidateWorkerQueues();
    }

    internal static void ValidateStrictFifoPolicy(
        KubeJob.Core.Runtime.ExecutionOrderingMode orderingMode,
        RabbitMqExecutionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (orderingMode != KubeJob.Core.Runtime.ExecutionOrderingMode.StrictFifo)
        {
            return;
        }

        if (!options.UseSingleActiveConsumer)
        {
            throw new InvalidOperationException(
                "StrictFifo RabbitMQ queues require x-single-active-consumer to be enabled.");
        }

        if (options.PrefetchCount != 1)
        {
            throw new InvalidOperationException(
                "StrictFifo RabbitMQ queues require PrefetchCount to be 1.");
        }

        if (options.ExecutionLaneCount != 1)
        {
            throw new InvalidOperationException(
                "StrictFifo RabbitMQ queues require ExecutionLaneCount to be 1 for global FIFO.");
        }
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
        ValidateWorkerQueues();
        DeclareGroupTopology(channel);
        DeclareRetryTopology(channel);
        if (_options.EnableCancelQueue)
        {
            DeclareCancelTopology(channel);
        }
        // Declare one physical dispatch queue per (logical queue, lane).
        // N=1 produces exactly today's queue set.
        for (var lane = 0; lane < _options.ExecutionLaneCount; lane++)
        {
            foreach (var logicalQueue in _worker.Queues)
            {
                DeclareDispatchQueue(channel, logicalQueue, lane);
            }
        }
    }

    private void ValidateWorkerQueues()
    {
        // The worker session registers with its own ConsumerGroup/ExecutionLane
        // (KubeJobWorkerOptions); the transport consumes under
        // RabbitMqExecutionOptions.ConsumerGroup. If the two disagree, every
        // broker admission silently fails the session/group match and the Run
        // spins in the retry queue — fail fast instead.
        if (!string.Equals(_worker.ConsumerGroup, _options.ConsumerGroup, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Worker session group '{_worker.ConsumerGroup}' does not match the RabbitMQ consumer group '{_options.ConsumerGroup}'. " +
                "Set KubeJobWorkerOptions.ConsumerGroup to the same group the transport is provisioned for.");
        }

        foreach (var logicalQueue in _worker.Queues)
        {
            var route = _queueCatalog.Resolve(logicalQueue);
            ValidateStrictFifoPolicy(route.Target.OrderingMode, _options);
            if (!string.Equals(route.Target.ConsumerGroup, _options.ConsumerGroup, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Worker group '{_options.ConsumerGroup}' cannot consume queue '{route.Queue}' " +
                    $"because QueueCatalog assigns it to group '{route.Target.ConsumerGroup}'.");
            }

            if (!string.Equals(route.Target.ExecutionLane, _worker.ExecutionLane, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Worker lane '{_worker.ExecutionLane}' cannot consume queue '{route.Queue}' " +
                    $"because QueueCatalog assigns it to lane '{route.Target.ExecutionLane}'.");
            }
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

        // No x-dead-letter-routing-key is set: RabbitMQ preserves the original
        // (lane-suffixed) routing key on dead-letter, so a retried message
        // re-lands on the same lane dispatch queue after the TTL expires.
        var retryArguments = new Dictionary<string, object>
        {
            ["x-queue-type"] = "quorum",
            ["x-message-ttl"] = checked((int)_options.RetryDelay.TotalMilliseconds),
            ["x-dead-letter-exchange"] = _options.GetGroupExchangeName()
        };
        // One group-shared retry queue binds every business queue/lane routing
        // key. Normal backlog remains isolated in the business dispatch queue;
        // this queue holds only short-lived broker admission retries.
        var retryQueue = _options.GetSharedRetryQueueName();
        channel.QueueDeclare(
            queue: retryQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: retryArguments);
        for (var lane = 0; lane < _options.ExecutionLaneCount; lane++)
        {
            foreach (var logicalQueue in _worker.Queues)
            {
                channel.QueueBind(
                    queue: retryQueue,
                    exchange: _options.GetRetryExchangeName(),
                    routingKey: _options.GetLaneRoutingKey(logicalQueue, lane),
                    arguments: null);
            }
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

    private void DeclareDispatchQueue(IModel channel, string logicalQueue, int lane)
    {
        var queueName = _options.GetConsumerQueueName(logicalQueue, lane);
        var arguments = new Dictionary<string, object>
        {
            ["x-queue-type"] = "quorum",
            ["x-dead-letter-exchange"] = _options.GetGroupDlxName()
        };
        if (_options.DefaultDeliveryLimit > 0)
        {
            arguments["x-delivery-limit"] = _options.DefaultDeliveryLimit;
        }
        if (_options.UseSingleActiveConsumer)
        {
            arguments["x-single-active-consumer"] = true;
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
            routingKey: _options.GetLaneRoutingKey(logicalQueue, lane),
            arguments: null);

        var routingKey = _options.GetLaneRoutingKey(logicalQueue, lane);
        if (Encoding.UTF8.GetByteCount(routingKey) >= 255)
        {
            throw new InvalidOperationException(
                $"RabbitMQ execution routing keys must be shorter than 255 UTF-8 bytes; got '{routingKey}'.");
        }
    }
}
