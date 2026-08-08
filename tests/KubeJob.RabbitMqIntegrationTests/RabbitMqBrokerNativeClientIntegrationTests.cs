using FluentAssertions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace KubeJob.RabbitMqIntegrationTests;

[Collection(RabbitMqIntegrationCollection.Name)]
public sealed class RabbitMqBrokerNativeClientIntegrationTests
{
    private static readonly JobKey<OrderCreatedPayload> JobKey = new("order.created");

    [Fact]
    public async Task JobClient_routes_broker_native_queue_directly_to_transport_without_creating_run()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            "KUBEJOB_RABBITMQ_TEST_CONNECTION")
            ?? throw new InvalidOperationException(
                "Set KUBEJOB_RABBITMQ_TEST_CONNECTION before running this integration project.");

        var suffix = Guid.NewGuid().ToString("N");
        var logicalQueue = "order.created";
        var exchange = $"kubejob.test.client.jobs.{suffix}";
        var queuePrefix = $"kubejob.test.client.{suffix}";
        var probe = new ExecutionProbe();
        var transportOptions = new RabbitMqBrokerNativeOptions
        {
            ConnectionString = connectionString,
            ExchangeName = exchange,
            QueuePrefix = queuePrefix,
            PrefetchCount = 8,
            RetryDelay = TimeSpan.FromMilliseconds(100),
            ReconnectDelay = TimeSpan.FromMilliseconds(100)
        };

        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddSingleton(probe);
                services.AddKubeJobServer();
                services.ConfigureKubeJobQueueRuntimes(options =>
                {
                    options.Queues[logicalQueue] = new QueueRuntimeRoute
                    {
                        Mode = QueueRuntimeMode.BrokerNative,
                        TransportId = RabbitMqBrokerNativePublisher.Id
                    };
                });
                services.AddKubeJobHandler<OrderCreatedJob, OrderCreatedPayload>(JobKey);
                services.AddKubeJobBrokerNativeWorker(options =>
                {
                    options.WorkerId = $"native-client-worker-{suffix}";
                    options.BuildId = "integration";
                    options.Queues = new List<string> { logicalQueue };
                    options.MaxConcurrentJobs = 8;
                });
                services.AddRabbitMqKubeJobBrokerNativeConsumer(options =>
                {
                    options.ConnectionString = transportOptions.ConnectionString;
                    options.ExchangeName = transportOptions.ExchangeName;
                    options.QueuePrefix = transportOptions.QueuePrefix;
                    options.PrefetchCount = transportOptions.PrefetchCount;
                    options.RetryDelay = transportOptions.RetryDelay;
                    options.ReconnectDelay = transportOptions.ReconnectDelay;
                });
            })
            .Build();

        await host.StartAsync();
        try
        {
            var physicalQueue = transportOptions.GetQueueName(logicalQueue);
            await EventuallyAsync(
                () => HasConsumerAsync(connectionString, physicalQueue),
                attempts: 300);

            var client = host.Services.GetRequiredService<IJobClient>();
            var handle = await client.EnqueueAsync(
                JobKey,
                new OrderCreatedPayload(1001));

            await probe.Completed.Task.WaitAsync(TimeSpan.FromSeconds(20));
            probe.ExecutionCount.Should().Be(1);

            // This is the producer-side architectural assertion: a
            // BrokerNative submission is a self-contained broker message, not
            // a PostgreSQL Run waiting for later managed claim.
            var queryStore = host.Services.GetRequiredService<IJobQueryStore>();
            var run = await queryStore.GetRunAsync(handle.JobId, CancellationToken.None);
            run.Should().BeNull();

            await EventuallyAsync(
                async () => await GetMessageCountAsync(connectionString, physicalQueue) == 0,
                attempts: 200);
        }
        finally
        {
            await host.StopAsync();
            CleanupTopology(connectionString, transportOptions, logicalQueue);
        }
    }

    private static IConnection CreateConnection(string connectionString)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(connectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = false
        };
        return factory.CreateConnection("KubeJob.BrokerNative.ClientIntegrationTest");
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

    private static void CleanupTopology(
        string connectionString,
        RabbitMqBrokerNativeOptions options,
        string logicalQueue)
    {
        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        TryDeleteQueue(channel, options.GetQueueName(logicalQueue));
        TryDeleteQueue(channel, options.GetRetryQueueName());
        TryDeleteQueue(channel, options.GetDeadLetterQueueName());
        TryDeleteExchange(channel, options.GetRetryExchangeName());
        TryDeleteExchange(channel, options.GetDeadLetterExchangeName());
        TryDeleteExchange(channel, options.ExchangeName);
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

    private sealed record OrderCreatedPayload(int OrderId);

    private sealed class ExecutionProbe
    {
        private int _executionCount;

        public int ExecutionCount => Volatile.Read(ref _executionCount);

        public TaskCompletionSource Completed { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Complete()
        {
            Interlocked.Increment(ref _executionCount);
            Completed.TrySetResult();
        }
    }

    private sealed class OrderCreatedJob : IKubeJob<OrderCreatedPayload>
    {
        private readonly ExecutionProbe _probe;

        public OrderCreatedJob(ExecutionProbe probe) => _probe = probe;

        public ValueTask ExecuteAsync(
            OrderCreatedPayload payload,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            _probe.Complete();
            return ValueTask.CompletedTask;
        }
    }
}
