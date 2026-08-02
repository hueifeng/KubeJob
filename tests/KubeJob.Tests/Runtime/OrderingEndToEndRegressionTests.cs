using System.Collections.Concurrent;
using System.Text.Json;
using FluentAssertions;
using KubeJob.Core.Attributes;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Data;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using RabbitMQ.Client;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// End-to-end ordering regression suite. The PostgreSQL KeyOrdered claim gate is
/// the ordering authority; these tests exercise it through the full durable
/// pipeline (business submit -> transactional outbox -> RabbitMQ execution
/// dispatch -> worker admission/claim/lease/completion) with real broker
/// delivery, retry, and lease-takeover, and assert the gate's behavior rather
/// than the broker's delivery order.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class OrderingEndToEndCollection
{
    public const string Name = "e2e-ordering";
}

[Collection(OrderingEndToEndCollection.Name)]
public sealed class OrderingEndToEndRegressionTests : IAsyncLifetime
{
    private const string LogicalQueue = "e2e.ordering";
    private const string QueuePrefix = "kubejob.test.e2e.ordering";

    private static readonly JobKey<OrderingPayload> OrderingJobKey = new("e2e.ordering");
    private static readonly JsonSerializerOptions WebJson = new(JsonSerializerDefaults.Web);

    private string? _postgresConnectionString;
    private string? _rabbitConnectionString;
    private string? _adminConnectionString;
    private string? _databaseName;
    private string? _testConnectionString;

    private bool Enabled => _postgresConnectionString is not null && _rabbitConnectionString is not null;

    public async Task InitializeAsync()
    {
        var postgres = Environment.GetEnvironmentVariable("KUBEJOB_TEST_POSTGRES");
        var rabbit = Environment.GetEnvironmentVariable("KUBEJOB_RABBITMQ_TEST_CONNECTION");
        if (string.IsNullOrWhiteSpace(postgres) || string.IsNullOrWhiteSpace(rabbit))
        {
            if (string.Equals(
                    Environment.GetEnvironmentVariable("KUBEJOB_REQUIRE_E2E_ORDERING"),
                    "1",
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "KUBEJOB_TEST_POSTGRES and KUBEJOB_RABBITMQ_TEST_CONNECTION are required for the end-to-end ordering suite.");
            }

            return;
        }

        _postgresConnectionString = postgres;
        _rabbitConnectionString = rabbit;
        _adminConnectionString = new NpgsqlConnectionStringBuilder(postgres)
        {
            Database = "postgres",
            Pooling = false
        }.ConnectionString;
        _databaseName = "kubejob_e2e_ordering_" + Guid.NewGuid().ToString("N");

        await using (var admin = new NpgsqlConnection(_adminConnectionString))
        {
            await admin.OpenAsync();
            await using var create = admin.CreateCommand();
            create.CommandText = $"CREATE DATABASE \"{_databaseName}\"";
            await create.ExecuteNonQueryAsync();
        }

        _testConnectionString = new NpgsqlConnectionStringBuilder(postgres)
        {
            Database = _databaseName,
            Pooling = true
        }.ConnectionString;
        new DbInitializer(_testConnectionString).Initialize();
    }

    public async Task DisposeAsync()
    {
        if (_adminConnectionString is null || _databaseName is null)
        {
            return;
        }

        await using var admin = new NpgsqlConnection(_adminConnectionString);
        await admin.OpenAsync();
        await using var terminate = admin.CreateCommand();
        terminate.CommandText =
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
            "WHERE datname = @database AND pid <> pg_backend_pid();";
        terminate.Parameters.AddWithValue("database", _databaseName);
        await terminate.ExecuteNonQueryAsync();

        await using var drop = admin.CreateCommand();
        drop.CommandText = $"DROP DATABASE IF EXISTS \"{_databaseName}\"";
        await drop.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Same ConcurrencyKey submitted in order (1, 2, 3). Run 1 fails its first
    /// attempt and retries. While run 1 is in-flight or retrying, runs 2 and 3
    /// must not execute; after run 1 succeeds, run 2 then run 3 execute strictly
    /// in submission order. Proves the KeyOrdered gate serializes per key across
    /// the durable retry boundary.
    /// </summary>
    [Fact]
    public async Task Same_concurrency_key_serializes_runs_across_retry()
    {
        if (!Enabled)
        {
            return;
        }

        var group = "e2e-order-same-" + Guid.NewGuid().ToString("N");
        var probe = new ExecutionProbe();
        using var host = BuildHost(
            probe,
            group,
            workerId: "worker-same",
            maxConcurrentJobs: 4,
            leaseRenewalInterval: TimeSpan.FromSeconds(10),
            heartbeatInterval: TimeSpan.FromSeconds(2),
            drainTimeout: TimeSpan.FromSeconds(2),
            leaseDuration: TimeSpan.FromSeconds(10),
            leaseReaperInterval: TimeSpan.FromMilliseconds(500));
        await host.StartAsync();
        try
        {
            var ingress = host.Services.GetRequiredService<IJobMessageIngressBatch>();
            var jobs = host.Services.GetRequiredService<IJobClient>();
            const string key = "order:same:1";
            var messages = new[]
            {
                Ingress("run-1", key, new OrderingPayload("run-1", "fail-then-succeed", FailAttempts: 1, HoldMs: 300, LongHoldMs: 0), maxAttempts: 3),
                Ingress("run-2", key, new OrderingPayload("run-2", "succeed", FailAttempts: 0, HoldMs: 100, LongHoldMs: 0), maxAttempts: 1),
                Ingress("run-3", key, new OrderingPayload("run-3", "succeed", FailAttempts: 0, HoldMs: 100, LongHoldMs: 0), maxAttempts: 1)
            };
            var results = await ingress.SubmitBatchAsync(messages);

            using var complete = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var s1 = await jobs.WaitForCompletionAsync(results[0].JobId, TimeSpan.FromMilliseconds(100), complete.Token);
            var s2 = await jobs.WaitForCompletionAsync(results[1].JobId, TimeSpan.FromMilliseconds(100), complete.Token);
            var s3 = await jobs.WaitForCompletionAsync(results[2].JobId, TimeSpan.FromMilliseconds(100), complete.Token);

            s1.Phase.Should().Be(JobPhase.Succeeded);
            s2.Phase.Should().Be(JobPhase.Succeeded);
            s3.Phase.Should().Be(JobPhase.Succeeded);
            s1.AttemptCount.Should().BeGreaterThanOrEqualTo(2, "run-1 must fail its first attempt and be retried by the durable outbox.");
            s2.AttemptCount.Should().Be(1);
            s3.AttemptCount.Should().Be(1);

            var events = probe.Snapshot();
            var tagOrder = events.OrderBy(e => e.StartSeq).Select(e => e.RunTag).ToArray();
            tagOrder.Should().Equal(new[] { "run-1", "run-1", "run-2", "run-3" },
                "the KeyOrdered gate admits a ConcurrencyKey strictly in submission order; " +
                "run-1's retry cannot be bypassed by run-2 or run-3.");

            var run1Attempts = events.Where(e => e.RunTag == "run-1").OrderBy(e => e.StartSeq).ToArray();
            run1Attempts.Should().HaveCount(2);
            run1Attempts[0].Succeeded.Should().BeFalse("the first attempt fails on purpose");
            run1Attempts[1].Succeeded.Should().BeTrue("the retry succeeds");

            // The gate, not broker delivery order, is authoritative: while run-1
            // is in-flight or retrying, run-2 and run-3 must not overlap with run-1's
            // successful attempt, nor with each other.
            AssertNoOverlap(run1Attempts[1], events.Single(e => e.RunTag == "run-2"));
            AssertNoOverlap(events.Single(e => e.RunTag == "run-2"), events.Single(e => e.RunTag == "run-3"));

            // TODO(item 1: lanes): assert same-lane-after-retry once ExecutionLaneCount>1.
            // The KeyOrdered gate currently serializes per ConcurrencyKey across the
            // single physical lane; once per-key lane partitioning ships, extend this
            // test to assert a retrying run's successor is admitted on the same lane as
            // its predecessor (same PartitionKey -> same lane routing key).
        }
        finally
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await host.StopAsync(stop.Token);
            DeleteGroupTopology(group);
        }
    }

    /// <summary>
    /// Different ConcurrencyKeys submitted concurrently must execute in parallel.
    /// The KeyOrdered gate only serializes runs sharing a ConcurrencyKey; distinct
    /// keys never block each other, so the gate is not a cross-key bottleneck.
    /// </summary>
    [Fact]
    public async Task Different_concurrency_keys_execute_in_parallel()
    {
        if (!Enabled)
        {
            return;
        }

        var group = "e2e-order-parallel-" + Guid.NewGuid().ToString("N");
        var probe = new ExecutionProbe();
        using var host = BuildHost(
            probe,
            group,
            workerId: "worker-parallel",
            maxConcurrentJobs: 4,
            leaseRenewalInterval: TimeSpan.FromSeconds(10),
            heartbeatInterval: TimeSpan.FromSeconds(2),
            drainTimeout: TimeSpan.FromSeconds(2),
            leaseDuration: TimeSpan.FromSeconds(10),
            leaseReaperInterval: TimeSpan.FromMilliseconds(500));
        await host.StartAsync();
        try
        {
            var ingress = host.Services.GetRequiredService<IJobMessageIngressBatch>();
            var jobs = host.Services.GetRequiredService<IJobClient>();
            var messages = new[]
            {
                Ingress("p-a", "order:parallel:a", new OrderingPayload("p-a", "succeed", FailAttempts: 0, HoldMs: 1500, LongHoldMs: 0), maxAttempts: 1),
                Ingress("p-b", "order:parallel:b", new OrderingPayload("p-b", "succeed", FailAttempts: 0, HoldMs: 1500, LongHoldMs: 0), maxAttempts: 1),
                Ingress("p-c", "order:parallel:c", new OrderingPayload("p-c", "succeed", FailAttempts: 0, HoldMs: 1500, LongHoldMs: 0), maxAttempts: 1)
            };
            var results = await ingress.SubmitBatchAsync(messages);

            using var complete = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            foreach (var result in results)
            {
                var status = await jobs.WaitForCompletionAsync(result.JobId, TimeSpan.FromMilliseconds(100), complete.Token);
                status.Phase.Should().Be(JobPhase.Succeeded);
            }

            var events = probe.Snapshot();
            events.Should().HaveCount(3, "each different-key run executes exactly once.");
            probe.MaxConcurrent.Should().BeGreaterThanOrEqualTo(2,
                "different ConcurrencyKeys must not serialize against each other under the KeyOrdered gate.");
            var maxStart = events.Max(e => e.StartedAt);
            var minFinish = events.Min(e => e.FinishedAt!.Value);
            maxStart.Should().BeBefore(minFinish,
                "all three different-key runs should be in flight simultaneously (no cross-key blocking).");
        }
        finally
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await host.StopAsync(stop.Token);
            DeleteGroupTopology(group);
        }
    }

    /// <summary>
    /// Broker-level redelivery of an in-flight run. A duplicate execution envelope
    /// is published straight to the shared group exchange while run 1 is still
    /// owned; the worker admits it, the gate reports the run is already running,
    /// and the duplicate cycles until the run is terminal. Ordering still holds and
    /// run 1 executes exactly once, proving the gate (not delivery order) is
    /// authoritative.
    /// </summary>
    [Fact]
    public async Task Broker_redelivery_of_in_flight_run_keeps_ordering_authoritative()
    {
        if (!Enabled)
        {
            return;
        }

        var group = "e2e-order-redeliver-" + Guid.NewGuid().ToString("N");
        var probe = new ExecutionProbe();
        var rabbit = new RabbitMqExecutionOptions
        {
            ConnectionString = _rabbitConnectionString!,
            ConsumerGroup = group,
            ConsumerQueuePrefix = QueuePrefix
        };
        using var host = BuildHost(
            probe,
            group,
            workerId: "worker-redeliver",
            maxConcurrentJobs: 4,
            leaseRenewalInterval: TimeSpan.FromSeconds(10),
            heartbeatInterval: TimeSpan.FromSeconds(2),
            drainTimeout: TimeSpan.FromSeconds(2),
            leaseDuration: TimeSpan.FromSeconds(10),
            leaseReaperInterval: TimeSpan.FromMilliseconds(500));
        await host.StartAsync();
        try
        {
            var ingress = host.Services.GetRequiredService<IJobMessageIngressBatch>();
            var jobs = host.Services.GetRequiredService<IJobClient>();
            const string key = "order:redeliver:7";
            var messages = new[]
            {
                Ingress("run-1", key, new OrderingPayload("run-1", "succeed", FailAttempts: 0, HoldMs: 2000, LongHoldMs: 0), maxAttempts: 1),
                Ingress("run-2", key, new OrderingPayload("run-2", "succeed", FailAttempts: 0, HoldMs: 100, LongHoldMs: 0), maxAttempts: 1),
                Ingress("run-3", key, new OrderingPayload("run-3", "succeed", FailAttempts: 0, HoldMs: 100, LongHoldMs: 0), maxAttempts: 1)
            };
            var results = await ingress.SubmitBatchAsync(messages);
            var run1 = results[0].JobId;

            await EventuallyAsync(async () =>
            {
                var status = await jobs.GetStatusAsync(run1);
                return status is not null && status.Phase == JobPhase.Running;
            });
            PublishDuplicateEnvelope(rabbit, run1);

            using var complete = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var s1 = await jobs.WaitForCompletionAsync(results[0].JobId, TimeSpan.FromMilliseconds(100), complete.Token);
            var s2 = await jobs.WaitForCompletionAsync(results[1].JobId, TimeSpan.FromMilliseconds(100), complete.Token);
            var s3 = await jobs.WaitForCompletionAsync(results[2].JobId, TimeSpan.FromMilliseconds(100), complete.Token);

            s1.Phase.Should().Be(JobPhase.Succeeded);
            s2.Phase.Should().Be(JobPhase.Succeeded);
            s3.Phase.Should().Be(JobPhase.Succeeded);
            s1.AttemptCount.Should().Be(1, "the duplicate delivery must be deduped by the gate; run-1 executes exactly once.");

            var events = probe.Snapshot();
            var tagOrder = events.OrderBy(e => e.StartSeq).Select(e => e.RunTag).ToArray();
            tagOrder.Should().Equal(new[] { "run-1", "run-2", "run-3" },
                "the control-plane gate, not broker delivery order, is authoritative for KeyOrdered runs.");
            AssertNoOverlap(events.Single(e => e.RunTag == "run-1"), events.Single(e => e.RunTag == "run-2"));
            AssertNoOverlap(events.Single(e => e.RunTag == "run-2"), events.Single(e => e.RunTag == "run-3"));
        }
        finally
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await host.StopAsync(stop.Token);
            DeleteGroupTopology(group);
        }
    }

    /// <summary>
    /// Worker crash/lease-loss takeover. Worker A claims run 1 and holds it long
    /// past its lease without renewing (simulating a stalled worker). The lease
    /// reaper requeues run 1; worker B takes over the retry and completes it.
    /// Runs 2 and 3 still wait for run 1 to become terminal before executing, and
    /// run 3 waits for run 2: same-key ordering and no same-key concurrent
    /// execution survive the takeover.
    /// </summary>
    [Fact]
    public async Task Worker_crash_takeover_preserves_same_key_ordering()
    {
        if (!Enabled)
        {
            return;
        }

        var group = "e2e-order-crash-" + Guid.NewGuid().ToString("N");
        var probe = new ExecutionProbe();
        // Worker A: one slot and a renewal interval far longer than the lease, so
        // its lease on run-1 expires before it ever renews (simulating a worker
        // that stalled mid-run). MaxConcurrentJobs=1 keeps its slot occupied by
        // the stuck attempt so A cannot pick up the requeued run.
        using var hostA = BuildHost(
            probe,
            group,
            workerId: "worker-crash-a",
            maxConcurrentJobs: 1,
            leaseRenewalInterval: TimeSpan.FromSeconds(30),
            heartbeatInterval: TimeSpan.FromMilliseconds(200),
            drainTimeout: TimeSpan.FromMilliseconds(500),
            leaseDuration: TimeSpan.FromSeconds(1),
            leaseReaperInterval: TimeSpan.FromMilliseconds(300));
        // Worker B: a normal survivor that takes over the requeued run and the rest.
        using var hostB = BuildHost(
            probe,
            group,
            workerId: "worker-crash-b",
            maxConcurrentJobs: 4,
            leaseRenewalInterval: TimeSpan.FromMilliseconds(400),
            heartbeatInterval: TimeSpan.FromMilliseconds(200),
            drainTimeout: TimeSpan.FromSeconds(2),
            leaseDuration: TimeSpan.FromSeconds(1),
            leaseReaperInterval: TimeSpan.FromMilliseconds(300));
        await hostA.StartAsync();
        var hostBStarted = false;
        try
        {
            var ingress = hostA.Services.GetRequiredService<IJobMessageIngressBatch>();
            var jobs = hostA.Services.GetRequiredService<IJobClient>();
            const string key = "order:crash:9";
            var messages = new[]
            {
                Ingress("run-1", key, new OrderingPayload("run-1", "hold", FailAttempts: 0, HoldMs: 100, LongHoldMs: 6000), maxAttempts: 3),
                Ingress("run-2", key, new OrderingPayload("run-2", "succeed", FailAttempts: 0, HoldMs: 100, LongHoldMs: 0), maxAttempts: 1),
                Ingress("run-3", key, new OrderingPayload("run-3", "succeed", FailAttempts: 0, HoldMs: 100, LongHoldMs: 0), maxAttempts: 1)
            };
            var results = await ingress.SubmitBatchAsync(messages);
            var run1 = results[0].JobId;

            // Wait until run-1 is owned and executing on worker A, then start B.
            await EventuallyAsync(async () =>
            {
                var status = await jobs.GetStatusAsync(run1);
                return status is not null
                    && status.Phase == JobPhase.Running
                    && string.Equals(status.CurrentWorkerId, "worker-crash-a", StringComparison.Ordinal);
            });
            await hostB.StartAsync();
            hostBStarted = true;

            using var complete = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var s1 = await jobs.WaitForCompletionAsync(results[0].JobId, TimeSpan.FromMilliseconds(100), complete.Token);
            var s2 = await jobs.WaitForCompletionAsync(results[1].JobId, TimeSpan.FromMilliseconds(100), complete.Token);
            var s3 = await jobs.WaitForCompletionAsync(results[2].JobId, TimeSpan.FromMilliseconds(100), complete.Token);

            s1.Phase.Should().Be(JobPhase.Succeeded, "the requeued run-1 must be completed by the survivor worker B.");
            s2.Phase.Should().Be(JobPhase.Succeeded);
            s3.Phase.Should().Be(JobPhase.Succeeded);
            s1.AttemptCount.Should().BeGreaterThanOrEqualTo(2, "run-1's first attempt must be lease-lost and retried after takeover.");

            var events = probe.Snapshot();
            var run1Events = events.Where(e => e.RunTag == "run-1").OrderBy(e => e.StartSeq).ToArray();
            run1Events.Should().HaveCountGreaterThanOrEqualTo(2);
            run1Events[0].WorkerId.Should().Be("worker-crash-a", "run-1's first attempt runs on the crashed worker A.");

            // The probe records handler-level completion, so worker A's stalled
            // attempt also records a "success" after its 6s hold — even though its
            // durable completion is fenced (lease-lost). The durable winner is the
            // highest attempt number, which by construction runs on the survivor.
            var run1Success = run1Events.OrderBy(e => e.AttemptNumber).Last(e => e.Succeeded);
            run1Success.WorkerId.Should().Be("worker-crash-b", "the successful retry must run on the survivor worker B.");

            // Ordering is preserved by the gate: run-2 starts only after run-1 is
            // terminal, and run-3 after run-2. The orphaned attempt on worker A is
            // already LeaseLost in the durable store, so it does not count against
            // same-key concurrency.
            var run2 = events.Single(e => e.RunTag == "run-2");
            var run3 = events.Single(e => e.RunTag == "run-3");
            run2.StartedAt.Should().BeAfter(run1Success.FinishedAt!.Value, "run-2 must wait for run-1 to become terminal after takeover.");
            run3.StartedAt.Should().BeAfter(run2.FinishedAt!.Value, "run-3 must wait for run-2 to become terminal.");
            AssertNoOverlap(run1Success, run2);
            AssertNoOverlap(run2, run3);
        }
        finally
        {
            if (hostBStarted)
            {
                using var stopB = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await hostB.StopAsync(stopB.Token);
            }

            using var stopA = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await hostA.StopAsync(stopA.Token);
            DeleteGroupTopology(group);
        }
    }

    private IHost BuildHost(
        ExecutionProbe probe,
        string group,
        string workerId,
        int maxConcurrentJobs,
        TimeSpan leaseRenewalInterval,
        TimeSpan heartbeatInterval,
        TimeSpan drainTimeout,
        TimeSpan leaseDuration,
        TimeSpan leaseReaperInterval)
    {
        var rabbit = new RabbitMqExecutionOptions
        {
            ConnectionString = _rabbitConnectionString!,
            ConsumerGroup = group,
            ConsumerQueuePrefix = QueuePrefix,
            MaxBrokerRetryAttempts = 100,
            RetryDelay = TimeSpan.FromMilliseconds(100),
            BrokerRetryReconciliationDelay = TimeSpan.FromSeconds(2),
            PrefetchCount = 16
        };

        return new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
                services.AddSingleton(probe);
                services.AddKubeJobHandler<OrderingJob, OrderingPayload>(OrderingJobKey);

                // Replicates the meta AddKubeJob helper: control plane + in-process
                // worker transport + worker in one process. KubeJob.Tests does not
                // reference the meta KubeJob package, so the four calls are spelled
                // out here against the referenced KubeJob.Server/Worker assemblies.
                services.AddKubeJobServer(server => server.UsePostgreSql(_testConnectionString!));
                services.UseInProcessKubeJobWorkerTransport();
                services.AddKubeJobWorker(worker =>
                {
                    worker.WorkerId = workerId;
                    worker.ConsumerGroup = group;
                    worker.Queues = new List<string> { LogicalQueue };
                    worker.MaxConcurrentJobs = maxConcurrentJobs;
                    worker.ClaimBatchSize = Math.Max(1, maxConcurrentJobs);
                    worker.EmptyPollDelay = TimeSpan.FromSeconds(30);
                    worker.LeaseRenewalInterval = leaseRenewalInterval;
                    worker.HeartbeatInterval = heartbeatInterval;
                    worker.DrainTimeout = drainTimeout;
                });

                services.ConfigureKubeJobQueueRouting(routing =>
                {
                    routing.Defaults.Profile = ExecutionDeliveryProfile.BrokerDispatch;
                    routing.Defaults.OrderingMode = ExecutionOrderingMode.KeyOrdered;
                    routing.Defaults.ConsumerGroup = group;
                });

                services.UseRabbitMqKubeJobExecutionDispatcher(options => CopyRabbit(rabbit, options));
                services.AddRabbitMqKubeJobExecutionConsumer(options => CopyRabbit(rabbit, options));

                services.Configure<JobRuntimeOptions>(options =>
                {
                    options.LeaseDuration = leaseDuration;
                    options.LeaseReaperInterval = leaseReaperInterval;
                    options.OutboxPollInterval = TimeSpan.FromMilliseconds(50);
                    options.RetryPolicy = new RetryPolicy(
                        BackoffStrategy.Fixed,
                        TimeSpan.FromMilliseconds(200),
                        TimeSpan.FromMilliseconds(200));
                });
            })
            .Build();
    }

    private static void CopyRabbit(RabbitMqExecutionOptions source, RabbitMqExecutionOptions target)
    {
        target.ConnectionString = source.ConnectionString;
        target.ConsumerGroup = source.ConsumerGroup;
        target.ConsumerQueuePrefix = source.ConsumerQueuePrefix;
        target.MaxBrokerRetryAttempts = source.MaxBrokerRetryAttempts;
        target.RetryDelay = source.RetryDelay;
        target.BrokerRetryReconciliationDelay = source.BrokerRetryReconciliationDelay;
        target.PrefetchCount = source.PrefetchCount;
    }

    private static JobIngressMessage Ingress(
        string messageId,
        string concurrencyKey,
        OrderingPayload payload,
        int maxAttempts)
    {
        var request = new EnqueueJobRequest(
            JobKey: OrderingJobKey.Value,
            PayloadJson: JsonSerializer.Serialize(payload, WebJson),
            Queue: LogicalQueue,
            ConcurrencyKey: concurrencyKey,
            MaxAttempts: maxAttempts,
            TimeoutSeconds: 60);
        return new JobIngressMessage(Source: "e2e.ordering", MessageId: messageId, Job: request);
    }

    private void PublishDuplicateEnvelope(RabbitMqExecutionOptions options, string runId)
    {
        // Publish a duplicate execution envelope straight to the shared group
        // exchange to simulate a broker-level redelivery while the run is still
        // owned. The routing key is the bare logical queue name, which is the
        // lane-0 binding key when ExecutionLaneCount is at its default of 1.
        using var connection = new ConnectionFactory
        {
            Uri = new Uri(_rabbitConnectionString!, UriKind.Absolute)
        }.CreateConnection("KubeJob.Tests.E2EOrdering.Redeliver");
        using var channel = connection.CreateModel();
        var envelope = new ExecutionEnvelope
        {
            SchemaVersion = ExecutionEnvelope.CurrentSchemaVersion,
            EventId = "e2e-redeliver-" + Guid.NewGuid().ToString("N"),
            Queue = LogicalQueue,
            ExecutionLane = "default",
            ConsumerGroup = "default",
            RunId = runId
        };
        var properties = channel.CreateBasicProperties();
        properties.ContentType = "application/json";
        properties.MessageId = "e2e-redeliver-" + Guid.NewGuid().ToString("N");
        channel.BasicPublish(
            exchange: options.GetGroupExchangeName(),
            routingKey: LogicalQueue,
            basicProperties: properties,
            body: JsonSerializer.SerializeToUtf8Bytes(envelope, WebJson));
    }

    private void DeleteGroupTopology(string group)
    {
        if (_rabbitConnectionString is null)
        {
            return;
        }

        var options = new RabbitMqExecutionOptions
        {
            ConnectionString = _rabbitConnectionString,
            ConsumerGroup = group,
            ConsumerQueuePrefix = QueuePrefix
        };
        try
        {
            using var connection = new ConnectionFactory
            {
                Uri = new Uri(_rabbitConnectionString, UriKind.Absolute)
            }.CreateConnection("KubeJob.Tests.E2EOrdering.Cleanup");
            using var channel = connection.CreateModel();
            channel.QueueDelete(options.GetConsumerQueueName("default"), ifUnused: false, ifEmpty: false);
            channel.QueueDelete(options.GetSharedRetryQueueName(), ifUnused: false, ifEmpty: false);
            channel.QueueDelete(options.GetGroupDlqName(), ifUnused: false, ifEmpty: false);
            channel.ExchangeDelete(options.GetGroupExchangeName());
            channel.ExchangeDelete(options.GetRetryExchangeName());
            channel.ExchangeDelete(options.GetGroupDlxName());
        }
        catch
        {
            // Best-effort cleanup; leaving stray broker topology behind does not
            // fail the test run.
        }
    }

    private static async Task EventuallyAsync(Func<Task<bool>> condition, int attempts = 200)
    {
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(100);
        }

        (await condition()).Should().BeTrue();
    }

    private static void AssertNoOverlap(OrderingEvent a, OrderingEvent b)
    {
        a.FinishedAt.Should().NotBeNull($"event {a.RunTag}#{a.AttemptNumber} should have finished");
        b.FinishedAt.Should().NotBeNull($"event {b.RunTag}#{b.AttemptNumber} should have finished");
        var aFinish = a.FinishedAt!.Value;
        var bFinish = b.FinishedAt!.Value;
        var overlaps = a.StartedAt < bFinish && b.StartedAt < aFinish;
        overlaps.Should().BeFalse(
            $"runs '{a.RunTag}' (attempt {a.AttemptNumber}) and '{b.RunTag}' (attempt {b.AttemptNumber}) " +
            "share a ConcurrencyKey and must never execute concurrently.");
    }

    public sealed record OrderingPayload(
        string RunTag,
        string Scenario,
        int FailAttempts,
        int HoldMs,
        int LongHoldMs);

    private sealed class OrderingEvent
    {
        public string RunTag { get; }
        public string RunId { get; }
        public int AttemptNumber { get; }
        public string WorkerId { get; }
        public string SessionId { get; }
        public long StartSeq { get; }
        public DateTimeOffset StartedAt { get; }
        public DateTimeOffset? FinishedAt { get; set; }
        public bool Succeeded { get; set; }
        public bool Canceled { get; set; }

        public OrderingEvent(
            string runTag,
            string runId,
            int attemptNumber,
            string workerId,
            string sessionId,
            long startSeq,
            DateTimeOffset startedAt)
        {
            RunTag = runTag;
            RunId = runId;
            AttemptNumber = attemptNumber;
            WorkerId = workerId;
            SessionId = sessionId;
            StartSeq = startSeq;
            StartedAt = startedAt;
        }
    }

    private sealed class ExecutionProbe
    {
        private long _startSeq;
        private int _concurrent;
        private int _maxConcurrent;

        public ConcurrentQueue<OrderingEvent> Events { get; } = new();

        public int MaxConcurrent => Volatile.Read(ref _maxConcurrent);

        public OrderingEvent Begin(
            string runTag,
            string runId,
            int attemptNumber,
            string workerId,
            string sessionId)
        {
            var evt = new OrderingEvent(
                runTag,
                runId,
                attemptNumber,
                workerId,
                sessionId,
                Interlocked.Increment(ref _startSeq),
                DateTimeOffset.UtcNow);
            Events.Enqueue(evt);

            var current = Interlocked.Increment(ref _concurrent);
            int maximum;
            do
            {
                maximum = Volatile.Read(ref _maxConcurrent);
            }
            while (current > maximum && Interlocked.CompareExchange(ref _maxConcurrent, current, maximum) != maximum);

            return evt;
        }

        public void End(OrderingEvent evt, bool succeeded, bool canceled = false)
        {
            evt.FinishedAt = DateTimeOffset.UtcNow;
            evt.Succeeded = succeeded;
            evt.Canceled = canceled;
            Interlocked.Decrement(ref _concurrent);
        }

        public List<OrderingEvent> Snapshot() => Events.ToArray().ToList();
    }

    [KubeJob("e2e.ordering")]
    private sealed class OrderingJob : IKubeJob<OrderingPayload>
    {
        private readonly ExecutionProbe _probe;

        public OrderingJob(ExecutionProbe probe) => _probe = probe;

        public async ValueTask ExecuteAsync(
            OrderingPayload payload,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            var evt = _probe.Begin(
                payload.RunTag,
                context.RunId,
                context.AttemptNumber,
                context.Worker.WorkerId,
                context.Worker.SessionId);
            try
            {
                var holdMs = payload.Scenario == "hold" && context.AttemptNumber <= 1
                    ? payload.LongHoldMs
                    : payload.HoldMs;
                if (holdMs > 0)
                {
                    await Task.Delay(holdMs, cancellationToken);
                }

                if (payload.Scenario == "fail-then-succeed" && context.AttemptNumber <= payload.FailAttempts)
                {
                    throw new InvalidOperationException(
                        $"Simulated transient failure on attempt {context.AttemptNumber} for run {payload.RunTag}.");
                }

                _probe.End(evt, succeeded: true);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _probe.End(evt, succeeded: false, canceled: true);
                throw;
            }
            catch
            {
                _probe.End(evt, succeeded: false);
                throw;
            }
        }
    }
}