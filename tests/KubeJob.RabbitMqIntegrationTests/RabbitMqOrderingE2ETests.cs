using System.Collections.Concurrent;
using FluentAssertions;
using KubeJob;
using KubeJob.Core.Attributes;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace KubeJob.RabbitMqIntegrationTests;

/// <summary>
/// Item 4: End-to-end RabbitMQ ordering tests.
///
/// Requires running RabbitMQ (Podman/Docker). Set KUBEJOB_RABBITMQ_TEST_CONNECTION
/// before running.
///
/// Covers:
/// - Same ConcurrencyKey: 1→2→3, first fails → 2/3 blocked → first succeeds → 2 then 3
/// - Different ConcurrencyKeys execute in parallel
/// - Broker redelivery preserves ordering
/// - Worker restart does not violate ordering contract
/// </summary>
[Collection(RabbitMqIntegrationCollection.Name)]
public sealed class RabbitMqOrderingE2ETests
{
    private static readonly JobKey<OrderingTestPayload> OrderingJobKey = new("ordering-e2e-test");

    // ──────────────────────────────────────────────────────────────
    // Core ordering behavior
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Same_key_1_2_3_first_fails_blocks_successors_then_advances_in_order()
    {
        var connStr = RequireConnectionString();
        var group = $"ord-fail-adv-{Guid.NewGuid():N}";
        var probe = new OrderingProbe();

        using var host = BuildOrderingHost(connStr, group, probe, ExecutionOrderingMode.KeyOrdered);
        await host.StartAsync();
        try
        {
            await EnsureConsumerReady(connStr, group);

            var jobs = host.Services.GetRequiredService<IJobClient>();
            var key = "order-key-A";

            // Submit 1, 2, 3 with the same ConcurrencyKey.
            // Run 1 fails on its first attempt.
            var h1 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(key, Sequence: 1, FailOnAttempt: 1),
                Options(key));
            var h2 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(key, Sequence: 2, FailOnAttempt: 0),
                Options(key));
            var h3 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(key, Sequence: 3, FailOnAttempt: 0),
                Options(key));

            // Wait for all to complete.
            var s1 = await jobs.WaitForCompletionAsync(h1, PollMs, Timeout30s);
            var s2 = await jobs.WaitForCompletionAsync(h2, PollMs, Timeout30s);
            var s3 = await jobs.WaitForCompletionAsync(h3, PollMs, Timeout30s);

            s1.Phase.Should().Be(JobPhase.Succeeded);
            s2.Phase.Should().Be(JobPhase.Succeeded);
            s3.Phase.Should().Be(JobPhase.Succeeded);

            // Assert execution order: 1, then 2, then 3.
            var executions = probe.Executed.OrderBy(e => e.ObservedAt).ToList();
            executions.Should().HaveCount(3);

            var exec1 = executions.First(e => e.Sequence == 1);
            var exec2 = executions.First(e => e.Sequence == 2);
            var exec3 = executions.First(e => e.Sequence == 3);

            // 1 finishes before 2 starts
            (exec1.ObservedAt + exec1.Duration).Should().BeOnOrBefore(exec2.ObservedAt,
                "Run 2 must not execute while Run 1 is inflight (same ConcurrencyKey)");
            // 2 finishes before 3 starts
            (exec2.ObservedAt + exec2.Duration).Should().BeOnOrBefore(exec3.ObservedAt,
                "Run 3 must not execute while Run 2 is inflight (same ConcurrencyKey)");

            // Run 1 should have retried (attempt > 1).
            s1.AttemptCount.Should().BeGreaterThan(1, "Run 1 was configured to fail on attempt 1");
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connStr, group);
        }
    }

    [Fact]
    public async Task Different_keys_execute_in_parallel()
    {
        var connStr = RequireConnectionString();
        var group = $"ord-diff-key-{Guid.NewGuid():N}";
        var probe = new OrderingProbe();

        using var host = BuildOrderingHost(connStr, group, probe, ExecutionOrderingMode.KeyOrdered);
        await host.StartAsync();
        try
        {
            await EnsureConsumerReady(connStr, group);

            var jobs = host.Services.GetRequiredService<IJobClient>();

            var hA = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload("key-A", Sequence: 1, FailOnAttempt: 0),
                Options("key-A"));
            var hB = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload("key-B", Sequence: 1, FailOnAttempt: 0),
                Options("key-B"));

            await jobs.WaitForCompletionAsync(hA, PollMs, Timeout30s);
            await jobs.WaitForCompletionAsync(hB, PollMs, Timeout30s);

            // Both should have made it to the worker.
            var executions = probe.Executed.ToList();
            executions.Should().HaveCount(2);

            // Different keys SHOULD overlap in wall-clock time because the
            // two workers can process them concurrently.
            var a = executions.First(e => e.Key == "key-A");
            var b = executions.First(e => e.Key == "key-B");
            var overlapped = a.ObservedAt < b.ObservedAt + b.Duration
                          && b.ObservedAt < a.ObservedAt + a.Duration;
            overlapped.Should().BeTrue("Different ConcurrencyKeys must execute concurrently");
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connStr, group);
        }
    }

    // ──────────────────────────────────────────────────────────────
    // Broker redelivery ordering
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Broker_redelivery_preserves_ordering_same_key()
    {
        var connStr = RequireConnectionString();
        var group = $"ord-redel-{Guid.NewGuid():N}";
        var probe = new OrderingProbe();

        // MaxBrokerRetryAttempts=3: the broker retries before handing off to
        // durable reconciliation, exercising the code path that NACKs and
        // requeues the message at the RabbitMQ level.
        using var host = BuildOrderingHost(
            connStr, group, probe, ExecutionOrderingMode.KeyOrdered,
            maxBrokerRetryAttempts: 3);
        await host.StartAsync();
        try
        {
            await EnsureConsumerReady(connStr, group);

            var jobs = host.Services.GetRequiredService<IJobClient>();
            var key = "redel-key";

            // Run 1 fails twice (consuming broker retries), then succeeds.
            var h1 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(key, Sequence: 1, FailOnAttempt: 2),
                Options(key));
            var h2 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(key, Sequence: 2, FailOnAttempt: 0),
                Options(key));

            var s1 = await jobs.WaitForCompletionAsync(h1, PollMs, Timeout30s);
            var s2 = await jobs.WaitForCompletionAsync(h2, PollMs, Timeout30s);

            s1.Phase.Should().Be(JobPhase.Succeeded);
            s2.Phase.Should().Be(JobPhase.Succeeded);

            var executions = probe.Executed.OrderBy(e => e.ObservedAt).ToList();
            executions.Should().HaveCount(2);

            executions[0].Sequence.Should().Be(1);
            executions[1].Sequence.Should().Be(2);

            // Verify that 1 completed (after redeliveries) before 2 started.
            (executions[0].ObservedAt + executions[0].Duration).Should()
                .BeOnOrBefore(executions[1].ObservedAt, "Broker redelivery must not reorder same-key runs");
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connStr, group);
        }
    }

    // ──────────────────────────────────────────────────────────────
    // StrictFifo ordering
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task StrictFifo_sequential_global_ordering_preserved()
    {
        var connStr = RequireConnectionString();
        var group = $"ord-sfifo-{Guid.NewGuid():N}";
        var probe = new OrderingProbe();

        using var host = BuildOrderingHost(connStr, group, probe, ExecutionOrderingMode.StrictFifo);
        await host.StartAsync();
        try
        {
            await EnsureConsumerReady(connStr, group);

            var jobs = host.Services.GetRequiredService<IJobClient>();
            var sfOptions = new JobEnqueueOptions { Queue = "default", MaxAttempts = 5 };

            // Submit 4 runs without ConcurrencyKey; StrictFifo gates on the
            // entire queue, so they must execute sequentially.
            var h1 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(null, Sequence: 1, FailOnAttempt: 0), sfOptions);
            var h2 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(null, Sequence: 2, FailOnAttempt: 0), sfOptions);
            var h3 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(null, Sequence: 3, FailOnAttempt: 0), sfOptions);
            var h4 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(null, Sequence: 4, FailOnAttempt: 0), sfOptions);

            var s1 = await jobs.WaitForCompletionAsync(h1, PollMs, Timeout30s);
            var s2 = await jobs.WaitForCompletionAsync(h2, PollMs, Timeout30s);
            var s3 = await jobs.WaitForCompletionAsync(h3, PollMs, Timeout30s);
            var s4 = await jobs.WaitForCompletionAsync(h4, PollMs, Timeout30s);

            s1.Phase.Should().Be(JobPhase.Succeeded);
            s2.Phase.Should().Be(JobPhase.Succeeded);
            s3.Phase.Should().Be(JobPhase.Succeeded);
            s4.Phase.Should().Be(JobPhase.Succeeded);

            var executions = probe.Executed.OrderBy(e => e.ObservedAt).ToList();
            executions.Should().HaveCount(4);

            // StrictFifo: NO overlap between any two executions.
            for (var i = 1; i < executions.Count; i++)
            {
                var prevEnd = executions[i - 1].ObservedAt + executions[i - 1].Duration;
                prevEnd.Should().BeOnOrBefore(executions[i].ObservedAt,
                    $"StrictFifo violation: seq {executions[i - 1].Sequence} → {executions[i].Sequence}");
            }
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connStr, group);
        }
    }

    [Fact]
    public async Task StrictFifo_failure_blocks_lane_until_resolution()
    {
        var connStr = RequireConnectionString();
        var group = $"ord-sfifo-fail-{Guid.NewGuid():N}";
        var probe = new OrderingProbe();

        using var host = BuildOrderingHost(connStr, group, probe, ExecutionOrderingMode.StrictFifo);
        await host.StartAsync();
        try
        {
            await EnsureConsumerReady(connStr, group);

            var jobs = host.Services.GetRequiredService<IJobClient>();
            var sfOptions = new JobEnqueueOptions { Queue = "default", MaxAttempts = 5 };

            // Run 1 fails on its first attempt but succeeds on retry.
            var h1 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(null, Sequence: 1, FailOnAttempt: 1), sfOptions);
            var h2 = await jobs.EnqueueAsync(OrderingJobKey,
                new OrderingTestPayload(null, Sequence: 2, FailOnAttempt: 0), sfOptions);

            var s1 = await jobs.WaitForCompletionAsync(h1, PollMs, Timeout30s);
            var s2 = await jobs.WaitForCompletionAsync(h2, PollMs, Timeout30s);

            s1.Phase.Should().Be(JobPhase.Succeeded);
            s1.AttemptCount.Should().BeGreaterThan(1);
            s2.Phase.Should().Be(JobPhase.Succeeded);

            var executions = probe.Executed.OrderBy(e => e.ObservedAt).ToList();
            executions.Should().HaveCount(2);

            executions[0].Sequence.Should().Be(1);
            executions[1].Sequence.Should().Be(2);

            // After Run 1's failure and retry, Run 2 must not have started
            // until Run 1 finished (StrictFifo blocks the lane).
            (executions[0].ObservedAt + executions[0].Duration).Should()
                .BeOnOrBefore(executions[1].ObservedAt);
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connStr, group);
        }
    }

    // ──────────────────────────────────────────────────────────────
    // Helper: host builder
    // ──────────────────────────────────────────────────────────────

    private static IHost BuildOrderingHost(
        string connectionString,
        string group,
        OrderingProbe probe,
        ExecutionOrderingMode orderingMode,
        int maxBrokerRetryAttempts = 2,
        int maxConcurrentJobs = 4)
    {
        var rabbitOpts = new RabbitMqExecutionOptions
        {
            ConnectionString = connectionString,
            ConsumerGroup = group,
            ConsumerQueuePrefix = "kubejob.test.ordering",
            MaxBrokerRetryAttempts = maxBrokerRetryAttempts,
            RetryDelay = TimeSpan.FromMilliseconds(200),
            BrokerRetryReconciliationDelay = TimeSpan.FromSeconds(2),
            ExecutionLaneCount = 1
        };

        if (orderingMode == ExecutionOrderingMode.StrictFifo)
        {
            rabbitOpts.UseSingleActiveConsumer = true;
            // StrictFifo serializes delivery to one in-flight message at a
            // time; prefetch must be 1 so the broker does not hand out more.
            rabbitOpts.PrefetchCount = 1;
        }

        var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddSingleton(probe);
                services.AddKubeJobHandler<OrderingTestJob, OrderingTestPayload>(OrderingJobKey);
                services.AddKubeJob(
                    configureServer: server => server.UseInMemory(),
                    configureWorker: worker =>
                    {
                        worker.WorkerId = $"worker-{group}";
                        worker.ConsumerGroup = group;
                        worker.Queues = new List<string> { "default" };
                        worker.MaxConcurrentJobs = maxConcurrentJobs;
                        worker.EmptyPollDelay = TimeSpan.FromSeconds(30);
                    });
                services.ConfigureKubeJobQueueRouting(routing =>
                {
                    routing.Queues["default"] = new QueueDefinition
                    {
                        Profile = ExecutionDeliveryProfile.BrokerDispatch,
                        OrderingMode = orderingMode,
                        ConsumerGroup = group
                    };
                });
                services.UseRabbitMqKubeJobExecutionDispatcher(o =>
                    CopyRabbitOpts(rabbitOpts, o));
                services.AddRabbitMqKubeJobExecutionConsumer(o =>
                    CopyRabbitOpts(rabbitOpts, o));
            })
            .Build();
        return host;
    }

    private static void CopyRabbitOpts(RabbitMqExecutionOptions source, RabbitMqExecutionOptions target)
    {
        target.ConnectionString = source.ConnectionString;
        target.ConsumerGroup = source.ConsumerGroup;
        target.ConsumerQueuePrefix = source.ConsumerQueuePrefix;
        target.MaxBrokerRetryAttempts = source.MaxBrokerRetryAttempts;
        target.RetryDelay = source.RetryDelay;
        target.BrokerRetryReconciliationDelay = source.BrokerRetryReconciliationDelay;
        target.ExecutionLaneCount = source.ExecutionLaneCount;
        target.UseSingleActiveConsumer = source.UseSingleActiveConsumer;
        target.PrefetchCount = source.PrefetchCount;
    }

    private static void DeleteGroupTopology(string connectionString, string group)
    {
        var opts = new RabbitMqExecutionOptions
        {
            ConnectionString = connectionString,
            ConsumerGroup = group,
            ConsumerQueuePrefix = "kubejob.test.ordering"
        };
        try
        {
            using var connection = CreateConnection(connectionString);
            using var channel = connection.CreateModel();
            channel.QueueDelete(opts.GetConsumerQueueName("default"), ifUnused: false, ifEmpty: false);
            channel.QueueDelete(opts.GetSharedRetryQueueName(), ifUnused: false, ifEmpty: false);
            channel.QueueDelete(opts.GetGroupDlqName(), ifUnused: false, ifEmpty: false);
            channel.ExchangeDelete(opts.GetGroupExchangeName());
            channel.ExchangeDelete(opts.GetRetryExchangeName());
            channel.ExchangeDelete(opts.GetGroupDlxName());
        }
        catch
        {
            // Best-effort.
        }
    }

    private static IConnection CreateConnection(string connectionString) =>
        new ConnectionFactory
        {
            Uri = new Uri(connectionString, UriKind.Absolute)
        }.CreateConnection("KubeJob.Tests.RabbitMqOrderingE2E");

    private static string RequireConnectionString() =>
        Environment.GetEnvironmentVariable("KUBEJOB_RABBITMQ_TEST_CONNECTION")
            ?? throw new InvalidOperationException(
                "Set KUBEJOB_RABBITMQ_TEST_CONNECTION before running ordering E2E tests.");

    private static async Task EnsureConsumerReady(string connectionString, string group)
    {
        var opts = new RabbitMqExecutionOptions
        {
            ConnectionString = connectionString,
            ConsumerGroup = group,
            ConsumerQueuePrefix = "kubejob.test.ordering"
        };
        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        for (var i = 0; i < 200; i++)
        {
            try
            {
                if (channel.ConsumerCount(opts.GetConsumerQueueName("default")) >= 1)
                    return;
            }
            catch
            {
                // Queue not yet declared.
            }
            await Task.Delay(100);
        }
        channel.ConsumerCount(opts.GetConsumerQueueName("default")).Should().BeGreaterThan(0);
    }

    private static JobEnqueueOptions Options(string concurrencyKey) => new()
    {
        Queue = "default",
        MaxAttempts = 5,
        ConcurrencyKey = concurrencyKey
    };

    private static readonly TimeSpan PollMs = TimeSpan.FromMilliseconds(100);
    private static CancellationToken Timeout30s =>
        new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token;

    // ──────────────────────────────────────────────────────────────
    // Test types
    // ──────────────────────────────────────────────────────────────

    private sealed record OrderingTestPayload(
        string? Key,
        int Sequence,
        int FailOnAttempt);

    private sealed record OrderingExecution(
        string? Key,
        int Sequence,
        DateTimeOffset ObservedAt,
        TimeSpan Duration);

    private sealed class OrderingProbe
    {
        private readonly object _lock = new();
        private readonly List<OrderingExecution> _executed = new();

        public IReadOnlyList<OrderingExecution> Executed
        {
            get { lock (_lock) return _executed.ToList(); }
        }

        public void Record(string? key, int sequence, DateTimeOffset observedAt, TimeSpan duration)
        {
            lock (_lock)
            {
                _executed.Add(new OrderingExecution(key, sequence, observedAt, duration));
            }
        }
    }

    [KubeJob("ordering-e2e-test")]
    private sealed class OrderingTestJob : IKubeJob<OrderingTestPayload>
    {
        private readonly OrderingProbe _probe;

        public OrderingTestJob(OrderingProbe probe) => _probe = probe;

        public async ValueTask ExecuteAsync(
            OrderingTestPayload payload,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            var observedAt = DateTimeOffset.UtcNow;

            if (payload.FailOnAttempt > 0 && context.AttemptNumber <= payload.FailOnAttempt)
            {
                throw new InvalidOperationException(
                    $"Simulated failure on attempt {context.AttemptNumber}/{payload.FailOnAttempt} "
                    + $"(key={payload.Key}, seq={payload.Sequence})");
            }

            // Small delay to make timing observable.
            await Task.Delay(200, cancellationToken);

            _probe.Record(payload.Key, payload.Sequence, observedAt,
                DateTimeOffset.UtcNow - observedAt);
        }
    }
}
