using System.Text.Json;
using FluentAssertions;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace KubeJob.RabbitMqIntegrationTests;

[Collection(RabbitMqIntegrationCollection.Name)]
public sealed class RabbitMqBrokerNativeIntegrationTests
{
    private static readonly JobKey<OrderCreatedPayload> JobKey = new("order.created");

    [Fact]
    public async Task BrokerNative_executes_and_retries_without_control_plane_runtime()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            "KUBEJOB_RABBITMQ_TEST_CONNECTION")
            ?? throw new InvalidOperationException(
                "Set KUBEJOB_RABBITMQ_TEST_CONNECTION before running this integration project.");

        var suffix = Guid.NewGuid().ToString("N");
        var exchange = $"kubejob.test.native.jobs.{suffix}";
        var queuePrefix = $"kubejob.test.native.{suffix}";
        var logicalQueue = "order.created";
        var probe = new ExecutionProbe();
        var transportOptions = new RabbitMqBrokerNativeOptions
        {
            ConnectionString = connectionString,
            ExchangeName = exchange,
            QueuePrefix = queuePrefix,
            PrefetchCount = 8,
            RetryDelay = TimeSpan.FromMilliseconds(100)
        };

        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddSingleton(probe);
                services.AddKubeJobHandler<OrderCreatedJob, OrderCreatedPayload>(JobKey);
                services.AddKubeJobBrokerNativeWorker(options =>
                {
                    options.WorkerId = $"native-worker-{suffix}";
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
                    options.ReconnectDelay = TimeSpan.FromMilliseconds(100);
                });
            })
            .Build();

        // This is the architectural assertion: the BrokerNative-only worker
        // can be built without any Managed control-plane client/service.
        host.Services.GetService<IWorkerRuntimeClient>().Should().BeNull();

        await host.StartAsync();
        try
        {
            var physicalQueue = transportOptions.GetQueueName(logicalQueue);
            await EventuallyAsync(
                () => HasConsumerAsync(connectionString, physicalQueue),
                attempts: 300);

            using var connection = CreateConnection(connectionString);
            using var channel = connection.CreateModel();
            var message = new BrokerNativeJobMessage
            {
                MessageId = $"order-created-{suffix}",
                JobKey = JobKey.Value,
                Queue = logicalQueue,
                PayloadJson = JsonSerializer.Serialize(new OrderCreatedPayload(1001, FailFirstAttempt: true)),
                Attempt = 1,
                MaxAttempts = 3,
                TimeoutSeconds = 10,
                EnqueuedAt = DateTimeOffset.UtcNow,
                IdempotencyKey = "order:1001"
            };
            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.ContentType = "application/json";
            properties.Type = "kubejob.broker-native.job";
            properties.MessageId = message.MessageId;
            channel.BasicPublish(
                exchange: exchange,
                routingKey: logicalQueue,
                mandatory: true,
                basicProperties: properties,
                body: JsonSerializer.SerializeToUtf8Bytes(message));

            var completedAttempt = await probe.CompletedAttempt.Task.WaitAsync(TimeSpan.FromSeconds(20));
            completedAttempt.Should().Be(2,
                "the first handler attempt fails and must be republished through RabbitMQ retry without PostgreSQL admission");
            probe.ExecutionCount.Should().Be(2);

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
        return factory.CreateConnection("KubeJob.BrokerNative.IntegrationTest");
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

    private sealed record OrderCreatedPayload(int OrderId, bool FailFirstAttempt);

    private sealed class ExecutionProbe
    {
        private int _executionCount;

        public int ExecutionCount => Volatile.Read(ref _executionCount);

        public TaskCompletionSource<int> CompletedAttempt { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Increment() => Interlocked.Increment(ref _executionCount);
    }

    private sealed class OrderCreatedJob : IKubeJob<OrderCreatedPayload>
    {
        private readonly ExecutionProbe _probe;

        public OrderCreatedJob(ExecutionProbe probe)
        {
            _probe = probe;
        }

        public ValueTask ExecuteAsync(
            OrderCreatedPayload payload,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            var execution = _probe.Increment();
            if (payload.FailFirstAttempt && execution == 1)
            {
                throw new InvalidOperationException("transient order creation failure");
            }

            _probe.CompletedAttempt.TrySetResult(context.AttemptNumber);
            return ValueTask.CompletedTask;
        }
    }
}
