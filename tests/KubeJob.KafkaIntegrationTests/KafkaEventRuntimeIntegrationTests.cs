using FluentAssertions;
using System.Collections.Concurrent;
using Confluent.Kafka;
using KubeJob.Core.Events;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Core.Transport;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Transport.Kafka;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace KubeJob.KafkaIntegrationTests;

public sealed class KafkaEventRuntimeIntegrationTests
{
    private static readonly EventKey<OrderCreatedEvent> OrderCreated =
        EventKey<OrderCreatedEvent>.Create("order.events", "order.created");
    private static readonly JobKey<OrderCreatedJobPayload> OrderCreatedJob = new("order.created");

    [KafkaFact]
    public async Task Broker_native_job_is_delivered_from_its_logical_queue_topic()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var bootstrapServers = KafkaTestEnvironment.GetRequiredBootstrapServers();
        var topicPrefix = $"kubejob.test.jobs.{suffix}";
        var probe = new JobProbe();
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddSingleton(probe);
                services.AddKubeJobServer();
                services.ConfigureKubeJobQueueRuntimes(options =>
                {
                    options.Queues[OrderCreatedJob.Value] = new QueueRuntimeRoute
                    {
                        Mode = QueueRuntimeMode.BrokerNative,
                        TransportId = KafkaBrokerNativePublisher.Id
                    };
                });
                services.AddKubeJobHandler<OrderCreatedJobHandler, OrderCreatedJobPayload>(OrderCreatedJob);
                services.AddKubeJobBrokerNativeWorker(options =>
                {
                    options.WorkerId = $"job-worker-{suffix}";
                    options.BuildId = "integration";
                    options.Queues = [OrderCreatedJob.Value];
                    options.MaxConcurrentJobs = 4;
                });
                services.AddKafkaKubeJobBrokerNativeConsumer(options =>
                {
                    options.BootstrapServers = bootstrapServers;
                    options.Environment = suffix;
                    options.JobTopicPrefix = topicPrefix;
                    options.CreateTopicsOnStartup = true;
                    options.TopicPartitions = 3;
                    options.ReplicationFactor = 1;
                    options.ReconnectDelayMilliseconds = 100;
                });
            })
            .Build();

        await host.StartAsync();
        try
        {
            await EventuallyAsync(
                () => TopicExists(bootstrapServers, $"{topicPrefix}.{OrderCreatedJob.Value}"),
                attempts: 80,
                delayMilliseconds: 100);
            var client = host.Services.GetRequiredService<IJobClient>();
            await client.EnqueueAsync(OrderCreatedJob, new OrderCreatedJobPayload(1001));
            await probe.Completed.Task.WaitAsync(TimeSpan.FromSeconds(8));
            probe.ExecutionCount.Should().Be(1);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [KafkaFact]
    public async Task Event_fans_out_per_fixed_capability_and_retry_stays_in_failing_group()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var probe = new EventProbe();
        var inbox = new InMemoryEventInboxStore();
        var bootstrapServers = KafkaTestEnvironment.GetRequiredBootstrapServers();
        var eventTopic = $"kubejob.test.events.{suffix}";

        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddSingleton(probe);
                services.AddSingleton<IEventInboxStore>(inbox);
                services.AddKubeJobBrokerNativeWorker(options =>
                {
                    options.WorkerId = $"event-worker-{suffix}";
                    options.BuildId = "integration";
                    options.Queues = [];
                    options.MaxConcurrentJobs = 8;
                });
                services.AddKubeJobEventHandler<OrderCreatedEvent, BusinessOrderCreatedHandler>(OrderCreated, "data");
                services.AddKubeJobEventHandler<OrderCreatedEvent, OrderLogHandler>(OrderCreated, "log");
                services.AddOptions<EventRuntimeOptions>();
                services.Configure<EventRuntimeOptions>(options =>
                    options.Topics[OrderCreated.Topic] = KafkaBrokerNativePublisher.Id);
                services.TryAddSingleton<IMessageTransportRegistry, MessageTransportRegistry>();
                services.TryAddSingleton<IEventBus, DefaultEventBus>();
                services.AddKafkaKubeJobEventConsumer(options =>
                {
                    options.BootstrapServers = bootstrapServers;
                    options.Environment = suffix;
                    options.EventTopic = eventTopic;
                    options.CreateTopicsOnStartup = true;
                    options.TopicPartitions = 3;
                    options.ReplicationFactor = 1;
                    options.ReconnectDelayMilliseconds = 100;
                });
            })
            .Build();

        await host.StartAsync();
        try
        {
            await EventuallyAsync(
                () => TopicExists(bootstrapServers, eventTopic),
                attempts: 80,
                delayMilliseconds: 100);
            var eventBus = host.Services.GetRequiredService<IEventBus>();
            await eventBus.PublishAsync(
                OrderCreated,
                new OrderCreatedEvent(1001, FailBusinessFirstAttempt: true),
                new EventPublishOptions { MaxAttempts = 3, Timeout = TimeSpan.FromSeconds(10) });

            await probe.BusinessCompleted.Task.WaitAsync(TimeSpan.FromSeconds(12));
            await probe.LogCompleted.Task.WaitAsync(TimeSpan.FromSeconds(12));
            probe.BusinessExecutions.Should().Be(2, "only data should receive its retry record");
            probe.LogExecutions.Should().Be(1, "the log group receives only the original event");
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [KafkaFact]
    public async Task Event_fans_out_to_all_capabilities_and_same_capability_replicas_share_partitions()
    {
        const int eventCount = 36;
        var suffix = Guid.NewGuid().ToString("N");
        var bootstrapServers = KafkaTestEnvironment.GetRequiredBootstrapServers();
        var eventTopic = $"kubejob.test.scale.{suffix}";
        var probe = new DistributionProbe();
        var inbox = new InMemoryEventInboxStore();

        using var firstHost = CreateDistributionHost($"event-worker-a-{suffix}", includeAllCapabilities: true);
        using var secondHost = CreateDistributionHost($"event-worker-b-{suffix}", includeAllCapabilities: false);
        await firstHost.StartAsync();
        try
        {
            await EventuallyAsync(
                () => TopicExists(bootstrapServers, eventTopic),
                attempts: 80,
                delayMilliseconds: 100);
            await secondHost.StartAsync();
            try
            {
                // Give the shared data group time to complete its cooperative rebalance.
                await Task.Delay(500);
                var eventBus = firstHost.Services.GetRequiredService<IEventBus>();
                foreach (var orderId in Enumerable.Range(1, eventCount))
                {
                    await eventBus.PublishAsync(
                        OrderCreated,
                        new OrderCreatedEvent(orderId, FailBusinessFirstAttempt: false),
                        new EventPublishOptions
                        {
                            MaxAttempts = 1,
                            Timeout = TimeSpan.FromSeconds(10),
                            PartitionKey = $"order:{orderId}"
                        });
                }

                await EventuallyAsync(
                    () => probe.DataExecutions >= eventCount
                        && probe.LogExecutions >= eventCount
                        && probe.NotifyExecutions >= eventCount,
                    attempts: 160,
                    delayMilliseconds: 100);

                probe.DataWorkerIds.Count.Should().BeGreaterThanOrEqualTo(2,
                    "two data replicas in one consumer group should be assigned different partitions");
                probe.LogExecutions.Should().Be(eventCount);
                probe.NotifyExecutions.Should().Be(eventCount);
            }
            finally
            {
                await secondHost.StopAsync();
            }
        }
        finally
        {
            await firstHost.StopAsync();
        }

        IHost CreateDistributionHost(string workerId, bool includeAllCapabilities)
        {
            return new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddLogging();
                    services.AddSingleton(probe);
                    services.AddSingleton<IEventInboxStore>(inbox);
                    services.AddKubeJobBrokerNativeWorker(options =>
                    {
                        options.WorkerId = workerId;
                        options.BuildId = "integration";
                        options.Queues = [];
                        options.MaxConcurrentJobs = 8;
                    });
                    services.AddKubeJobEventHandler<OrderCreatedEvent, DistributedDataHandler>(OrderCreated, "data");
                    if (includeAllCapabilities)
                    {
                        services.AddKubeJobEventHandler<OrderCreatedEvent, DistributedLogHandler>(OrderCreated, "log");
                        services.AddKubeJobEventHandler<OrderCreatedEvent, DistributedNotifyHandler>(OrderCreated, "notify");
                    }

                    services.AddOptions<EventRuntimeOptions>();
                    services.Configure<EventRuntimeOptions>(options =>
                        options.Topics[OrderCreated.Topic] = KafkaBrokerNativePublisher.Id);
                    services.TryAddSingleton<IMessageTransportRegistry, MessageTransportRegistry>();
                    services.TryAddSingleton<IEventBus, DefaultEventBus>();
                    services.AddKafkaKubeJobEventConsumer(options =>
                    {
                        options.BootstrapServers = bootstrapServers;
                        options.Environment = suffix;
                        options.EventTopic = eventTopic;
                        options.CreateTopicsOnStartup = true;
                        options.TopicPartitions = 3;
                        options.ReplicationFactor = 1;
                        options.ReconnectDelayMilliseconds = 100;
                    });
                })
                .Build();
        }
    }

    private static bool TopicExists(string bootstrapServers, string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        return admin.GetMetadata(TimeSpan.FromSeconds(2)).Topics.Any(candidate =>
            string.Equals(candidate.Topic, topic, StringComparison.Ordinal)
            && candidate.Error.Code == ErrorCode.NoError);
    }

    private static async Task EventuallyAsync(Func<bool> predicate, int attempts, int delayMilliseconds)
    {
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            if (predicate())
            {
                return;
            }

            await Task.Delay(delayMilliseconds);
        }

        predicate().Should().BeTrue();
    }

    private sealed record OrderCreatedEvent(int OrderId, bool FailBusinessFirstAttempt);
    private sealed record OrderCreatedJobPayload(int OrderId);

    private sealed class EventProbe
    {
        private int _businessExecutions;
        private int _logExecutions;

        public int BusinessExecutions => Volatile.Read(ref _businessExecutions);
        public int LogExecutions => Volatile.Read(ref _logExecutions);
        public TaskCompletionSource BusinessCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource LogCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int IncrementBusiness() => Interlocked.Increment(ref _businessExecutions);
        public int IncrementLog() => Interlocked.Increment(ref _logExecutions);
    }

    private sealed class JobProbe
    {
        private int _executionCount;
        public int ExecutionCount => Volatile.Read(ref _executionCount);
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Complete()
        {
            Interlocked.Increment(ref _executionCount);
            Completed.TrySetResult();
        }
    }

    private sealed class DistributionProbe
    {
        private int _dataExecutions;
        private int _logExecutions;
        private int _notifyExecutions;
        private readonly ConcurrentDictionary<string, byte> _dataWorkerIds = [];

        public int DataExecutions => Volatile.Read(ref _dataExecutions);
        public int LogExecutions => Volatile.Read(ref _logExecutions);
        public int NotifyExecutions => Volatile.Read(ref _notifyExecutions);
        public IReadOnlyCollection<string> DataWorkerIds => _dataWorkerIds.Keys.ToArray();

        public void Data(EventExecutionContext context)
        {
            Interlocked.Increment(ref _dataExecutions);
            _dataWorkerIds.TryAdd(context.Worker.WorkerId, 0);
        }

        public void Log() => Interlocked.Increment(ref _logExecutions);
        public void Notify() => Interlocked.Increment(ref _notifyExecutions);
    }

    private sealed class OrderCreatedJobHandler(JobProbe probe) : IKubeJob<OrderCreatedJobPayload>
    {
        public ValueTask ExecuteAsync(OrderCreatedJobPayload payload, JobExecutionContext context, CancellationToken cancellationToken)
        {
            probe.Complete();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class BusinessOrderCreatedHandler(EventProbe probe) : IKubeEventHandler<OrderCreatedEvent>
    {
        public ValueTask HandleAsync(OrderCreatedEvent @event, EventExecutionContext context, CancellationToken cancellationToken)
        {
            var execution = probe.IncrementBusiness();
            if (@event.FailBusinessFirstAttempt && execution == 1)
            {
                throw new InvalidOperationException("transient data handler failure");
            }

            probe.BusinessCompleted.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class OrderLogHandler(EventProbe probe) : IKubeEventHandler<OrderCreatedEvent>
    {
        public ValueTask HandleAsync(OrderCreatedEvent @event, EventExecutionContext context, CancellationToken cancellationToken)
        {
            probe.IncrementLog();
            probe.LogCompleted.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DistributedDataHandler(DistributionProbe probe) : IKubeEventHandler<OrderCreatedEvent>
    {
        public ValueTask HandleAsync(OrderCreatedEvent @event, EventExecutionContext context, CancellationToken cancellationToken)
        {
            probe.Data(context);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DistributedLogHandler(DistributionProbe probe) : IKubeEventHandler<OrderCreatedEvent>
    {
        public ValueTask HandleAsync(OrderCreatedEvent @event, EventExecutionContext context, CancellationToken cancellationToken)
        {
            probe.Log();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DistributedNotifyHandler(DistributionProbe probe) : IKubeEventHandler<OrderCreatedEvent>
    {
        public ValueTask HandleAsync(OrderCreatedEvent @event, EventExecutionContext context, CancellationToken cancellationToken)
        {
            probe.Notify();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class InMemoryEventInboxStore : IEventInboxStore
    {
        private readonly ConcurrentDictionary<(string EventId, string ConsumerName), byte> _entries = [];

        public ValueTask<bool> IsProcessedAsync(string eventId, string consumerName, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(_entries.ContainsKey((eventId, consumerName)));

        public ValueTask MarkProcessedAsync(string eventId, string consumerName, CancellationToken cancellationToken = default)
        {
            _entries.TryAdd((eventId, consumerName), 0);
            return ValueTask.CompletedTask;
        }
    }

}
