using FluentAssertions;
using KubeJob.Core.Events;
using KubeJob.Core.Transport;
using KubeJob.Server.Runtime;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace KubeJob.RabbitMqIntegrationTests;

[Collection(RabbitMqIntegrationCollection.Name)]
public sealed class RabbitMqEventRuntimeIntegrationTests
{
    private static readonly EventKey<OrderCreatedEvent> OrderCreated =
        EventKey<OrderCreatedEvent>.Create("order.events", "order.created");

    [Fact]
    public async Task Event_fans_out_once_per_subscription_and_retry_is_subscription_scoped()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            "KUBEJOB_RABBITMQ_TEST_CONNECTION")
            ?? throw new InvalidOperationException(
                "Set KUBEJOB_RABBITMQ_TEST_CONNECTION before running this integration project.");

        var suffix = Guid.NewGuid().ToString("N");
        var prefix = $"kubejob.test.events.{suffix}";
        var probe = new EventProbe();
        var transportOptions = new RabbitMqBrokerNativeOptions
        {
            ConnectionString = connectionString,
            QueuePrefix = prefix,
            ExchangeName = $"{prefix}.jobs",
            PrefetchCount = 8,
            RetryDelay = TimeSpan.FromMilliseconds(100),
            ReconnectDelay = TimeSpan.FromMilliseconds(100)
        };

        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddSingleton(probe);
                services.AddKubeJobBrokerNativeWorker(options =>
                {
                    options.WorkerId = $"event-worker-{suffix}";
                    options.BuildId = "integration";
                    options.Queues = new List<string>();
                    options.MaxConcurrentJobs = 8;
                });
                services.AddKubeJobEventHandler<OrderCreatedEvent, BusinessOrderCreatedHandler>(
                    OrderCreated,
                    "order-business");
                services.AddKubeJobEventHandler<OrderCreatedEvent, OrderLogHandler>(
                    OrderCreated,
                    "order-log");

                services.AddOptions<EventRuntimeOptions>();
                services.Configure<EventRuntimeOptions>(options =>
                    options.Topics[OrderCreated.Topic] = RabbitMqBrokerNativePublisher.Id);
                services.TryAddSingleton<IMessageTransportRegistry, MessageTransportRegistry>();
                services.TryAddSingleton<IEventBus, DefaultEventBus>();

                services.AddRabbitMqKubeJobEventConsumer(options =>
                {
                    options.ConnectionString = transportOptions.ConnectionString;
                    options.QueuePrefix = transportOptions.QueuePrefix;
                    options.ExchangeName = transportOptions.ExchangeName;
                    options.PrefetchCount = transportOptions.PrefetchCount;
                    options.RetryDelay = transportOptions.RetryDelay;
                    options.ReconnectDelay = transportOptions.ReconnectDelay;
                });
            })
            .Build();

        // Event-only BrokerNative data plane must not require a Managed worker
        // client or a fake Job Queue just to satisfy old worker metadata rules.
        host.Services.GetService<KubeJob.Core.Runtime.IWorkerRuntimeClient>().Should().BeNull();

        await host.StartAsync();
        try
        {
            var businessQueue = transportOptions.GetEventSubscriptionQueueName(
                OrderCreated.Topic,
                "order-business");
            var logQueue = transportOptions.GetEventSubscriptionQueueName(
                OrderCreated.Topic,
                "order-log");
            await EventuallyAsync(
                async () => await HasConsumerAsync(connectionString, businessQueue)
                    && await HasConsumerAsync(connectionString, logQueue),
                attempts: 300);

            var eventBus = host.Services.GetRequiredService<IEventBus>();
            var handle = await eventBus.PublishAsync(
                OrderCreated,
                new OrderCreatedEvent(1001, FailBusinessFirstAttempt: true),
                new EventPublishOptions
                {
                    MaxAttempts = 3,
                    Timeout = TimeSpan.FromSeconds(10)
                });

            handle.EventId.Should().NotBeNullOrWhiteSpace();
            await probe.BusinessCompleted.Task.WaitAsync(TimeSpan.FromSeconds(20));
            await probe.LogCompleted.Task.WaitAsync(TimeSpan.FromSeconds(20));

            probe.BusinessExecutions.Should().Be(2,
                "the business subscription fails once and retries only itself");
            probe.LogExecutions.Should().Be(1,
                "a retry must return to the failing subscription queue, not republish to the Topic");

            await EventuallyAsync(
                async () => await GetMessageCountAsync(connectionString, businessQueue) == 0
                    && await GetMessageCountAsync(connectionString, logQueue) == 0,
                attempts: 200);
        }
        finally
        {
            await host.StopAsync();
            CleanupEventTopology(connectionString, transportOptions);
        }
    }

    private static IConnection CreateConnection(string connectionString)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(connectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = false
        };
        return factory.CreateConnection("KubeJob.EventRuntime.IntegrationTest");
    }

    private static Task<bool> HasConsumerAsync(string connectionString, string queue)
    {
        try
        {
            using var connection = CreateConnection(connectionString);
            using var channel = connection.CreateModel();
            return Task.FromResult(channel.ConsumerCount(queue) >= 1);
        }
        catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
        {
            return Task.FromResult(false);
        }
    }

    private static Task<uint> GetMessageCountAsync(string connectionString, string queue)
    {
        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        return Task.FromResult(channel.MessageCount(queue));
    }

    private static async Task EventuallyAsync(
        Func<Task<bool>> predicate,
        int attempts,
        int delayMilliseconds = 50)
    {
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            if (await predicate())
            {
                return;
            }

            await Task.Delay(delayMilliseconds);
        }

        (await predicate()).Should().BeTrue();
    }

    private static void CleanupEventTopology(
        string connectionString,
        RabbitMqBrokerNativeOptions options)
    {
        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        foreach (var subscription in new[] { "order-business", "order-log" })
        {
            TryDeleteQueue(channel, options.GetEventSubscriptionQueueName(OrderCreated.Topic, subscription));
            TryDeleteQueue(channel, options.GetEventRetryQueueName(OrderCreated.Topic, subscription));
            TryDeleteQueue(channel, options.GetEventDeadLetterQueueName(OrderCreated.Topic, subscription));
        }

        TryDeleteExchange(channel, options.GetEventRetryExchangeName(OrderCreated.Topic));
        TryDeleteExchange(channel, options.GetEventDeadLetterExchangeName(OrderCreated.Topic));
        TryDeleteExchange(channel, options.GetEventExchangeName(OrderCreated.Topic));
    }

    private static void TryDeleteQueue(IModel channel, string queue)
    {
        try
        {
            channel.QueueDelete(queue, ifUnused: false, ifEmpty: false);
        }
        catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
        {
        }
    }

    private static void TryDeleteExchange(IModel channel, string exchange)
    {
        try
        {
            channel.ExchangeDelete(exchange, ifUnused: false);
        }
        catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
        {
        }
    }

    private sealed record OrderCreatedEvent(int OrderId, bool FailBusinessFirstAttempt);

    private sealed class EventProbe
    {
        private int _businessExecutions;
        private int _logExecutions;

        public int BusinessExecutions => Volatile.Read(ref _businessExecutions);
        public int LogExecutions => Volatile.Read(ref _logExecutions);

        public TaskCompletionSource BusinessCompleted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource LogCompleted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int IncrementBusiness() => Interlocked.Increment(ref _businessExecutions);
        public int IncrementLog() => Interlocked.Increment(ref _logExecutions);
    }

    private sealed class BusinessOrderCreatedHandler : IKubeEventHandler<OrderCreatedEvent>
    {
        private readonly EventProbe _probe;

        public BusinessOrderCreatedHandler(EventProbe probe) => _probe = probe;

        public ValueTask HandleAsync(
            OrderCreatedEvent @event,
            EventExecutionContext context,
            CancellationToken cancellationToken)
        {
            var execution = _probe.IncrementBusiness();
            if (@event.FailBusinessFirstAttempt && execution == 1)
            {
                throw new InvalidOperationException("transient business subscriber failure");
            }

            _probe.BusinessCompleted.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class OrderLogHandler : IKubeEventHandler<OrderCreatedEvent>
    {
        private readonly EventProbe _probe;

        public OrderLogHandler(EventProbe probe) => _probe = probe;

        public ValueTask HandleAsync(
            OrderCreatedEvent @event,
            EventExecutionContext context,
            CancellationToken cancellationToken)
        {
            _probe.IncrementLog();
            _probe.LogCompleted.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }
}
