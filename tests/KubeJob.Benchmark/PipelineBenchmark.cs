using System.Collections.Concurrent;
using System.Diagnostics;
using KubeJob;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Server.Options;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Data;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using KubeJob.Worker.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using RabbitMQ.Client;

namespace KubeJob.Benchmark;

/// <summary>
/// Drives one scenario end to end through a unified control-plane + worker host
/// backed by PostgreSQL and RabbitMQ, then tears down the per-run database and
/// broker topology so the harness is repeatable. The host uses the in-process
/// worker transport for claim/lease/completion (no localhost HTTP) while
/// execution envelopes flow over RabbitMQ, isolating the broker dispatch path
/// as the transport under test.
/// </summary>
public sealed class PipelineBenchmark
{
    private const string ExecutionPrefix = "kubejob.bench.execution";
    private const string IngressPrefix = "kubejob.bench.ingress";

    private readonly BenchmarkOptions _opts;

    public PipelineBenchmark(BenchmarkOptions opts) => _opts = opts;

    /// <summary>
    /// Runs one scenario × laneCount. When <see cref="_opts"/> specifies multiple
    /// lane counts via <see cref="BenchmarkOptions.LaneCountSweep"/>, each produces
    /// a separate <see cref="ScenarioResult"/> whose <see cref="ScenarioResult.LaneCount"/>
    /// records the corresponding N.
    /// </summary>
    public async Task<IReadOnlyList<ScenarioResult>> RunScenarioAsync(BenchScenario scenario)
    {
        var results = new List<ScenarioResult>(_opts.LaneCountSweep.Count);
        foreach (var laneCount in _opts.LaneCountSweep)
        {
            // Temporarily reset ExecutionLaneCount for this sweep pass.
            // (BenchmarkOptions is mutable via setters for CLI override.)
            var savedLaneCounts = _opts.LaneCountSweep;
            try
            {
                _opts.LaneCountSweep = new[] { laneCount };
                results.Add(await RunScenarioSingleAsync(scenario, laneCount));
            }
            finally
            {
                _opts.LaneCountSweep = savedLaneCounts;
            }
        }
        return results;
    }

    /// <summary>
    /// Runs one scenario: provision a fresh database, build and start the host,
    /// warm up, submit the measured batch, wait for completion, sample metrics,
    /// then dispose everything. Exceptions propagate so a failing scenario is
    /// visible in the report.
    /// </summary>
    private async Task<ScenarioResult> RunScenarioSingleAsync(BenchScenario scenario, int laneCount)
    {
        var group = $"bench-{scenario.Label().ToLowerInvariant()}-{Guid.NewGuid():N}";
        var topology = BuildTopology(group, scenario);
        var runSw = Stopwatch.StartNew();

        var (benchConnStr, dbName) = await CreateFreshDatabaseAsync();
        // Turn off synchronous_commit at the database level so that COMMIT does not
        // wait for WAL fsync. Safe for benchmarking: the DB is per-run scratch.
        await SetSynchronousCommitOffAsync(benchConnStr);
        IHost? host = null;
        MetricsSampler? sampler = null;
        IConnection? rabbitConnection = null;

        try
        {
            // No Reset On Close skips the DISCARD ALL round-trip when returning a
            // pooled connection, cutting one statement per lifecycle.
            host = BuildHost(benchConnStr + ";No Reset On Close=true", scenario, group, topology);
            // Initialize schema before starting hosted services so the outbox
            // publisher, lease reaper, and worker find a ready database.
            InitializeSchema(benchConnStr);
            await host.StartAsync();

            if (_opts.DeliveryProfile == ExecutionDeliveryProfile.BrokerDispatch)
            {
                rabbitConnection = OpenRabbitConnection();
                await WaitForRabbitReadyAsync(rabbitConnection, topology);
            }

            if (_opts.Warmup > 0)
            {
                if (rabbitConnection != null)
                    await WarmupAsync(host, scenario, topology, rabbitConnection);
                else
                    await PullWarmupAsync(host, scenario);
            }

            // Start the metrics sampler immediately before the measured submit
            // so it captures the ramp and steady-state of the run under test.
            sampler = new MetricsSampler(
                benchConnStr,
                _opts.RabbitMqManagementUri,
                _opts.RabbitMqUser,
                _opts.RabbitMqPassword,
                topology.MetricsQueues,
                _opts.CpuSamplingEnabled ? _opts.PostgresContainerName : null,
                TimeSpan.FromMilliseconds(_opts.MetricsIntervalMs));

            // Two wall clocks: submitSw is the ingest phase only (drives Ingest
            // TPS); e2eWallSw spans submit-start to completion-detected and drives
            // E2E TPS (wall). runSw keeps the whole-scenario duration (including
            // provisioning) as an informational column, NOT a TPS denominator.
            var submitSw = Stopwatch.StartNew();
            var e2eWallSw = Stopwatch.StartNew();
            await SubmitAsync(host, scenario, scenario.QueueName(), topology, rabbitConnection);
            submitSw.Stop();

            var completion = await WaitForCompletionAsync(host, scenario.QueueName(), _opts.JobCount);
            e2eWallSw.Stop();
            runSw.Stop();

            var metrics = sampler.Snapshot();
            return BuildResult(scenario, laneCount, completion, submitSw, e2eWallSw, runSw, metrics);
        }
        finally
        {
            if (sampler is not null) await sampler.DisposeAsync();
            if (host is not null)
            {
                using (host)
                {
                    try
                    {
                        using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                        await host.StopAsync(stopCts.Token);
                    }
                    catch { /* best-effort */ }
                }
            }
            rabbitConnection?.Dispose();
            await DropDatabaseAsync(dbName);
            DeleteTopology(topology);
        }
    }

    // --- Topology ---

    private sealed record BenchTopology(
        string Group,
        string SharedExecutionQueue,
        string SharedRetryQueue,
        string GroupExchange,
        string RetryExchange,
        string GroupDlx,
        string GroupDlq,
        string? IngressExchange,
        string? IngressQueue,
        /// <summary>Per-lane execution queue names (lane-0..N-1). Empty when laneCount==1 (shared queue).</summary>
        IReadOnlyList<string> LaneExecutionQueues,
        /// <summary>Per-lane retry queue names. Empty when laneCount==1.</summary>
        IReadOnlyList<string> LaneRetryQueues)
    {
        /// <summary>Queues the metrics sampler watches (execution always; ingress when used).</summary>
        public IReadOnlyList<string> MetricsQueues
        {
            get
            {
                var queues = new List<string>();
                if (LaneExecutionQueues.Count > 0)
                    queues.AddRange(LaneExecutionQueues);
                else
                    queues.Add(SharedExecutionQueue);
                if (IngressQueue is not null)
                    queues.Add(IngressQueue);
                return queues;
            }
        }

        /// <summary>All execution queues for readiness checks.</summary>
        public IReadOnlyList<string> ExecutionQueues =>
            LaneExecutionQueues.Count > 0 ? LaneExecutionQueues : new[] { SharedExecutionQueue };
    }

    private BenchTopology BuildTopology(string group, BenchScenario scenario)
    {
        // Derive every physical name from the transport's naming contract so
        // readiness checks, metrics, and cleanup always agree with the queue
        // the consumer actually consumes.
        var naming = new RabbitMqExecutionOptions
        {
            ConsumerGroup = group,
            ConsumerQueuePrefix = ExecutionPrefix,
            ExecutionLaneCount = Math.Max(1, _opts.ExecutionLaneCount)
        };
        var queue = scenario.QueueName();
        var sharedQueue = naming.GetConsumerQueueName(queue, 0);
        var sharedRetryQueue = naming.GetSharedRetryQueueName();
        var groupExchange = naming.GetGroupExchangeName();
        var retryExchange = naming.GetRetryExchangeName();
        var groupDlx = naming.GetGroupDlxName();
        var groupDlq = naming.GetGroupDlqName();

        var laneExecutionQueues = new List<string>();
        if (_opts.ExecutionLaneCount > 1)
        {
            for (var lane = 0; lane < _opts.ExecutionLaneCount; lane++)
            {
                laneExecutionQueues.Add(naming.GetConsumerQueueName(queue, lane));
            }
        }

        string? ingressExchange = null;
        string? ingressQueue = null;
        if (_opts.SubmissionMode == SubmissionMode.Ingress)
        {
            ingressExchange = $"{IngressPrefix}.{group}.exchange";
            ingressQueue = $"{IngressPrefix}.{group}.queue";
        }

        return new BenchTopology(
            group, sharedQueue, sharedRetryQueue, groupExchange, retryExchange,
            groupDlx, groupDlq, ingressExchange, ingressQueue,
            laneExecutionQueues, LaneRetryQueues: Array.Empty<string>());
    }

    // --- Host wiring ---

    private IHost BuildHost(string benchConnStr, BenchScenario scenario, string group, BenchTopology topology)
    {
        var execOptions = new RabbitMqExecutionOptions
        {
            ConnectionString = _opts.RabbitMqConnectionString,
            ConsumerGroup = group,
            ConsumerQueuePrefix = ExecutionPrefix,
            PrefetchCount = (ushort)Math.Max(1, _opts.PrefetchCount),
            AdmissionBatchSize = Math.Clamp(_opts.AdmissionBatchSize, 1, 256),
            ConsumerDispatchConcurrency = (ushort)Math.Max(1, _opts.ConsumerDispatchConcurrency),
            PublisherConcurrency = Math.Clamp(_opts.PublisherConcurrency, 1, 32),
            ExecutionLaneCount = _opts.ExecutionLaneCount
        };

        // StrictFIFO: single active consumer + prefetch=1 per lane
        if (scenario == BenchScenario.StrictFifo)
        {
            execOptions.UseSingleActiveConsumer = true;
            execOptions.PrefetchCount = 1;
        }

        var queue = scenario.QueueName();
        var workerId = $"bench-worker-{group}";

        // Background pool must clear OutboxPublishConcurrency + 3 fixed loops;
        // add headroom so the outbox publisher never starves the reaper/retention.
        var backgroundPool = Math.Max(16, _opts.OutboxPublishConcurrency + 16);
        var businessPool = Math.Max(48, _opts.SubmitterConcurrency + _opts.WorkerMaxConcurrency + 48);

        var host = new HostBuilder()
            .ConfigureLogging(builder => builder
                .AddConsole()
                .SetMinimumLevel(LogLevel.Warning))
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddKubeJob(
                    configureServer: server => server.UsePostgreSql(
                        benchConnStr,
                        storage =>
                        {
                            storage.BusinessPoolSize = businessPool;
                            storage.BackgroundPoolSize = backgroundPool;
                        }),
                    configureWorker: worker =>
                    {
                        worker.WorkerId = workerId;
                        worker.BuildId = "bench";
                        worker.ConsumerGroup = group;
                        // The measured queue plus a separate warmup queue so warmup
                        // Runs do not contaminate the measured completion sweep,
                        // which filters by the measured queue alone.
                        worker.Queues = new List<string> { queue, WarmupQueue(queue) };
                        worker.MaxConcurrentJobs = _opts.WorkerMaxConcurrency;
                        worker.ClaimBatchSize = _opts.DeliveryProfile == ExecutionDeliveryProfile.Pull
                            ? Math.Min(256, _opts.WorkerMaxConcurrency)
                            : Math.Min(64, _opts.WorkerMaxConcurrency);
                        worker.EmptyPollDelay = _opts.DeliveryProfile == ExecutionDeliveryProfile.Pull
                            ? TimeSpan.FromMilliseconds(5)
                            : TimeSpan.FromMilliseconds(100);
                        worker.HeartbeatInterval = TimeSpan.FromSeconds(1);
                        worker.LeaseRenewalInterval = TimeSpan.FromSeconds(1);
                        worker.DrainTimeout = TimeSpan.FromSeconds(5);
                    });

                services.ConfigureKubeJobQueueRouting(routing =>
                {
                    routing.Defaults.Profile = _opts.DeliveryProfile;
                    routing.Defaults.OrderingMode = ExecutionOrderingMode.Parallel;
                    routing.Queues[queue] = new QueueDefinition
                    {
                        Profile = _opts.DeliveryProfile,
                        OrderingMode = scenario.OrderingMode(),
                        ConsumerGroup = group
                    };
                    routing.Queues[WarmupQueue(queue)] = new QueueDefinition
                    {
                        Profile = _opts.DeliveryProfile,
                        ConsumerGroup = group
                    };
                });

                if (_opts.DeliveryProfile == ExecutionDeliveryProfile.BrokerDispatch)
                {
                    services.UseRabbitMqKubeJobExecutionDispatcher(o => CopyExecOptions(execOptions, o));
                    services.AddRabbitMqKubeJobExecutionConsumer(o => CopyExecOptions(execOptions, o));
                }

                if (_opts.SubmissionMode == SubmissionMode.Ingress)
                {
                    services.AddRabbitMqKubeJobIngress(o =>
                    {
                        o.ConnectionString = _opts.RabbitMqConnectionString;
                        o.ExchangeName = topology.IngressExchange!;
                        o.QueueName = topology.IngressQueue!;
                        o.RoutingKey = "#";
                        o.Source = "kubejob-bench";
                        o.AllowNoDeadLetterExchange = true;
                        o.PrefetchCount = (ushort)Math.Max(1, _opts.IngressPrefetch);
                        o.SubmissionBatchSize = Math.Max(1, _opts.IngressBatchSize);
                        o.SubmissionBatchWait = TimeSpan.FromMilliseconds(Math.Max(1, _opts.IngressBatchWaitMs));
                    });
                }

                services.AddSingleton(new BenchJobOptions { WorkMs = _opts.JobWorkMs });
                services.AddKubeJobHandler<NoopBenchJob, BenchPayload>(NoopBenchJob.JobKey);

                services.Configure<JobRuntimeOptions>(runtime =>
                {
                    runtime.OutboxPublishConcurrency = _opts.OutboxPublishConcurrency;
                    runtime.OutboxBatchSize = _opts.OutboxBatchSize;
                    runtime.OutboxPollInterval = TimeSpan.FromMilliseconds(_opts.OutboxPollIntervalMs);
                    runtime.CompletionBatchSize = Math.Min(256, _opts.WorkerMaxConcurrency);
                    runtime.CompletionBatcherShardCount = _opts.DeliveryProfile == ExecutionDeliveryProfile.Pull
                        ? 8 : 4;
                    runtime.CompletionFlushInterval = TimeSpan.FromMilliseconds(
                        _opts.DeliveryProfile == ExecutionDeliveryProfile.Pull ? 2 : 10);
                    runtime.LeaseDuration = TimeSpan.FromMinutes(2);
                    runtime.MaxClaimBatchSize = _opts.DeliveryProfile == ExecutionDeliveryProfile.Pull
                        ? 1024 : 256;
                });
            })
            .Build();
        return host;
    }

    private static void CopyExecOptions(RabbitMqExecutionOptions source, RabbitMqExecutionOptions target)
    {
        target.ConnectionString = source.ConnectionString;
        target.ConsumerGroup = source.ConsumerGroup;
        target.ConsumerQueuePrefix = source.ConsumerQueuePrefix;
        target.PrefetchCount = source.PrefetchCount;
        target.ConsumerDispatchConcurrency = source.ConsumerDispatchConcurrency;
        target.PublisherConcurrency = source.PublisherConcurrency;
        target.ExecutionLaneCount = source.ExecutionLaneCount;
        target.UseSingleActiveConsumer = source.UseSingleActiveConsumer;
    }

    private void InitializeSchema(string benchConnStr)
    {
        // Use a non-pooled connection for initialization so no idle connection
        // lingers against the bench database before it is later dropped.
        var noPool = new NpgsqlConnectionStringBuilder(benchConnStr) { Pooling = false }.ConnectionString;
        new DbInitializer(noPool).Initialize();
    }

    // --- Rabbit readiness ---

    private IConnection OpenRabbitConnection() =>
        new ConnectionFactory { Uri = new Uri(_opts.RabbitMqConnectionString, UriKind.Absolute) }
            .CreateConnection("kubejob-bench");

    private static async Task WaitForRabbitReadyAsync(IConnection connection, BenchTopology topology)
    {
        using var channel = connection.CreateModel();
        // Wait for consumers on all execution queues (per-lane or shared).
        foreach (var queue in topology.ExecutionQueues)
        {
            await EventuallyAsync(
                () => channel.ConsumerCount(queue) >= 1,
                TimeSpan.FromSeconds(30));
        }
        if (topology.IngressQueue is not null)
        {
            await EventuallyAsync(
                () => channel.ConsumerCount(topology.IngressQueue) >= 1,
                TimeSpan.FromSeconds(30));
        }
    }

    private static async Task EventuallyAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (condition()) return;
            }
            catch (Exception ex) when (ex is RabbitMQ.Client.Exceptions.OperationInterruptedException)
            {
                // Queue not yet declared – keep waiting.
            }
            await Task.Delay(100);
        }
        try
        {
            if (condition()) return;
        }
        catch (Exception ex) when (ex is RabbitMQ.Client.Exceptions.OperationInterruptedException)
        {
            // Last-chance check also failed because queue still doesn't exist.
        }
        throw new TimeoutException("Timed out waiting for RabbitMQ consumer readiness.");
    }

    // --- Submission ---

    private Task SubmitAsync(IHost host, BenchScenario scenario, string queue, BenchTopology? topology, IConnection? connection)
    {
        return _opts.SubmissionMode == SubmissionMode.Ingress
            ? SubmitIngressAsync(scenario, queue, topology ?? throw new InvalidOperationException("Ingress benchmark topology is required."), connection ?? throw new InvalidOperationException("Ingress benchmark connection is required."))
            : SubmitTypedAsync(host, scenario, queue);
    }

    private async Task SubmitTypedAsync(IHost host, BenchScenario scenario, string queue)
    {
        var client = host.Services.GetRequiredService<IJobClient>();
        const int batchSize = 100;

        for (var batch = 0; batch < _opts.JobCount; batch += batchSize)
        {
            var end = Math.Min(batch + batchSize, _opts.JobCount);
            var batchItems = new List<(BenchPayload, JobEnqueueOptions?)>(end - batch);
            for (var i = batch; i < end; i++)
            {
                var key = scenario.ConcurrencyKey(i, _opts.HotKeyCardinality, _opts.UniformKeyCardinality);
                batchItems.Add((new BenchPayload(i), new JobEnqueueOptions
                {
                    Queue = queue,
                    ConcurrencyKey = key,
                    MaxAttempts = 1,
                    Timeout = TimeSpan.FromMinutes(2)
                }));
            }
            await client.EnqueueBatchAsync(NoopBenchJob.JobKey, batchItems);
        }
    }

    private async Task SubmitIngressAsync(BenchScenario scenario, string queue, BenchTopology topology, IConnection connection)
    {
        var parallelism = Math.Max(1, _opts.SubmitterConcurrency);
        using var pool = new RabbitPublishPool(connection, parallelism);
        var options = new ParallelOptions { MaxDegreeOfParallelism = parallelism };

        await Parallel.ForEachAsync(Enumerable.Range(0, _opts.JobCount), options, async (i, ct) =>
        {
            var envelope = new RabbitMqJobIngressEnvelope(
                MessageId: $"{topology.Group}-{i}",
                JobKey: NoopBenchJob.BenchJobKeyString,
                PayloadJson: "{}",
                Queue: queue,
                ConcurrencyKey: scenario.ConcurrencyKey(i, _opts.HotKeyCardinality, _opts.UniformKeyCardinality),
                MaxAttempts: 1,
                TimeoutSeconds: 120);
            var body = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(envelope);
            await pool.PublishAsync(
                topology.IngressExchange!,
                "bench.noop",
                body,
                envelope.MessageId,
                ct);
        });
    }

    private static string WarmupQueue(string measuredQueue) => $"{measuredQueue}.warmup";

    private async Task WarmupAsync(IHost host, BenchScenario scenario, BenchTopology topology, IConnection connection)
    {
        var saved = _opts.JobCount;
        var savedWarmup = _opts.Warmup;
        var warmupQueue = WarmupQueue(scenario.QueueName());
        // Temporarily run a small batch through the same submit path on a
        // separate logical queue to prime DB pools, broker topology, and the
        // worker session without contaminating the measured completion sweep.
        _opts.JobCount = savedWarmup;
        try
        {
            await SubmitAsync(host, scenario, warmupQueue, topology, connection);
            await WaitForCompletionAsync(host, warmupQueue, savedWarmup, TimeSpan.FromSeconds(60));
        }
        finally
        {
            _opts.JobCount = saved;
        }
    }

    private async Task PullWarmupAsync(IHost host, BenchScenario scenario)
    {
        var saved = _opts.JobCount;
        var warmupQueue = WarmupQueue(scenario.QueueName());
        _opts.JobCount = _opts.Warmup;
        try
        {
            await SubmitAsync(host, scenario, warmupQueue, null!, null!);
            await WaitForCompletionAsync(host, warmupQueue, _opts.Warmup, TimeSpan.FromSeconds(60));
        }
        finally
        {
            _opts.JobCount = saved;
        }
    }

    // --- Completion tracking (dashboard sweep, uniform for both submit modes) ---

    private sealed record CompletionResult(
        int Succeeded,
        int Failed,
        int CanceledOrDead,
        long MinCreatedAtTicks,
        long MaxCompletedAtTicks,
        double[] LatencySamplesMs);

    private async Task<CompletionResult> WaitForCompletionAsync(
        IHost host,
        string queue,
        int expected,
        TimeSpan? timeout = null)
    {
        var dash = host.Services.GetRequiredService<IJobRuntimeDashboardStore>();
        var collected = new ConcurrentDictionary<string, DashboardRunSummary>();
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(_opts.RunTimeoutSeconds));
        var ct = CancellationToken.None;

        while (collected.Count < expected && DateTimeOffset.UtcNow < deadline)
        {
            await PageAndCollectAsync(dash, queue, collected, ct);
            if (collected.Count >= expected) break;
            try { await Task.Delay(_opts.PollIntervalMs, ct); } catch (TaskCanceledException) { break; }
        }

        return BuildCompletion(expected, collected);
    }

    private static async Task PageAndCollectAsync(
        IJobRuntimeDashboardStore dash,
        string queue,
        ConcurrentDictionary<string, DashboardRunSummary> collected,
        CancellationToken ct)
    {
        // Page through every Run for the scenario queue. The dashboard query
        // returns the full filtered TotalCount (no recent-window cap), so this
        // scan is exhaustive for the per-scenario queue. Terminal runs are kept;
        // non-terminal runs are ignored and re-evaluated on the next sweep.
        var page = 1;
        while (true)
        {
            var result = await dash.GetRunsAsync(
                new DashboardRunQuery(Page: page, PageSize: 100, Queue: queue),
                ct);
            foreach (var item in result.Items)
            {
                if (IsTerminal(item.Phase))
                {
                    collected[item.Id] = item;
                }
            }
            var totalPages = result.TotalCount == 0 ? 1 : (int)Math.Ceiling(result.TotalCount / 100.0);
            if (page >= totalPages) break;
            page++;
        }
    }

    private static bool IsTerminal(JobPhase phase) =>
        phase is JobPhase.Succeeded or JobPhase.Failed or JobPhase.Canceled or JobPhase.Dead;

    private static CompletionResult BuildCompletion(int expected, ConcurrentDictionary<string, DashboardRunSummary> collected)
    {
        int succeeded = 0, failed = 0, canceledOrDead = 0;
        long minCreated = long.MaxValue;
        long maxCompleted = long.MinValue;
        var latencies = new List<double>(collected.Count);

        foreach (var run in collected.Values)
        {
            if (run.Phase == JobPhase.Succeeded) succeeded++;
            else if (run.Phase == JobPhase.Failed) failed++;
            else canceledOrDead++;

            if (run.CompletedAt is { } completed)
            {
                latencies.Add((completed - run.CreatedAt).TotalMilliseconds);
            }

            minCreated = Math.Min(minCreated, run.CreatedAt.UtcTicks);
            if (run.CompletedAt is { } c) maxCompleted = Math.Max(maxCompleted, c.UtcTicks);
        }

        var timedOut = expected - collected.Count;
        if (timedOut > 0) canceledOrDead += timedOut;

        return new CompletionResult(
            succeeded, failed, canceledOrDead,
            minCreated == long.MaxValue ? 0 : minCreated,
            maxCompleted == long.MinValue ? 0 : maxCompleted,
            latencies.ToArray());
    }

    // --- Result assembly ---

    private ScenarioResult BuildResult(
        BenchScenario scenario,
        int laneCount,
        CompletionResult completion,
        Stopwatch submitSw,
        Stopwatch e2eWallSw,
        Stopwatch runSw,
        MetricSamples metrics)
    {
        var ingestTps = submitSw.Elapsed.TotalSeconds > 0
            ? _opts.JobCount / submitSw.Elapsed.TotalSeconds : 0;

        var e2eTps = 0.0;
        if (completion.MaxCompletedAtTicks > 0 && completion.MinCreatedAtTicks > 0
            && completion.MaxCompletedAtTicks > completion.MinCreatedAtTicks)
        {
            var span = TimeSpan.FromTicks(completion.MaxCompletedAtTicks - completion.MinCreatedAtTicks);
            e2eTps = completion.Succeeded / span.TotalSeconds;
        }

        var wallClockE2eTps = e2eWallSw.Elapsed.TotalSeconds > 0
            ? _opts.JobCount / e2eWallSw.Elapsed.TotalSeconds : 0;

        return new ScenarioResult(
            scenario,
            _opts.JobCount,
            completion.Succeeded,
            completion.Failed,
            completion.CanceledOrDead,
            ingestTps,
            e2eTps,
            wallClockE2eTps,
            Percentiles.Compute(completion.LatencySamplesMs),
            metrics,
            runSw.Elapsed)
        { Mode = _opts.SubmissionMode.ToString(), LaneCount = laneCount };
    }

    // --- Database lifecycle ---

    private async Task<(string benchConnStr, string dbName)> CreateFreshDatabaseAsync()
    {
        var adminBuilder = new NpgsqlConnectionStringBuilder(_opts.PostgresConnectionString)
        {
            Database = "postgres",
            Pooling = false
        };
        var dbName = "kubejob_bench_" + Guid.NewGuid().ToString("N");
        await using (var admin = new NpgsqlConnection(adminBuilder.ConnectionString))
        {
            await admin.OpenAsync();
            await using var create = admin.CreateCommand();
            create.CommandText = $"CREATE DATABASE \"{dbName}\"";
            await create.ExecuteNonQueryAsync();
        }

        var benchBuilder = new NpgsqlConnectionStringBuilder(_opts.PostgresConnectionString)
        {
            Database = dbName,
            Pooling = true
        };
        return (benchBuilder.ConnectionString, dbName);
    }

    private static async Task SetSynchronousCommitOffAsync(string benchConnStr)
    {
        // ALTER DATABASE ... SET synchronous_commit = off persists for all new
        // sessions to the benchmark database. This eliminates WAL fsync on every
        // COMMIT, which is the single largest source of latency in a commit-heavy
        // benchmark. The trade-off is durability on crash — acceptable because the
        // database is a per-run scratch artifact.
        var builder = new NpgsqlConnectionStringBuilder(benchConnStr)
        {
            Pooling = false
        };
        await using var conn = new NpgsqlConnection(builder.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"ALTER DATABASE {conn.Database} SET synchronous_commit = off";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task DropDatabaseAsync(string dbName)
    {
        var adminBuilder = new NpgsqlConnectionStringBuilder(_opts.PostgresConnectionString)
        {
            Database = "postgres",
            Pooling = false
        };
        try
        {
            await using var admin = new NpgsqlConnection(adminBuilder.ConnectionString);
            await admin.OpenAsync();
            await using var terminate = admin.CreateCommand();
            terminate.CommandText =
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
                "WHERE datname = @db AND pid <> pg_backend_pid();";
            terminate.Parameters.AddWithValue("db", dbName);
            await terminate.ExecuteNonQueryAsync();

            await using var drop = admin.CreateCommand();
            drop.CommandText = $"DROP DATABASE IF EXISTS \"{dbName}\"";
            await drop.ExecuteNonQueryAsync();
        }
        catch
        {
            // A failed drop must not mask the benchmark results; the next run
            // uses a fresh database name anyway.
        }
    }

    // --- Broker topology cleanup ---

    private void DeleteTopology(BenchTopology topology)
    {
        try
        {
            using var connection = OpenRabbitConnection();
            using var channel = connection.CreateModel();
            if (topology.IngressQueue is not null)
            {
                channel.QueueDelete(topology.IngressQueue, ifUnused: false, ifEmpty: false);
            }
            if (topology.IngressExchange is not null)
            {
                channel.ExchangeDelete(topology.IngressExchange);
            }
            // Clean up per-lane queues first, then shared queues.
            foreach (var laneQueue in topology.LaneExecutionQueues)
                channel.QueueDelete(laneQueue, ifUnused: false, ifEmpty: false);
            foreach (var laneRetry in topology.LaneRetryQueues)
                channel.QueueDelete(laneRetry, ifUnused: false, ifEmpty: false);
            channel.QueueDelete(topology.SharedExecutionQueue, ifUnused: false, ifEmpty: false);
            channel.QueueDelete(topology.SharedRetryQueue, ifUnused: false, ifEmpty: false);
            channel.QueueDelete(topology.GroupDlq, ifUnused: false, ifEmpty: false);
            channel.ExchangeDelete(topology.GroupExchange);
            channel.ExchangeDelete(topology.RetryExchange);
            channel.ExchangeDelete(topology.GroupDlx);
        }
        catch
        {
            // Best-effort: stray broker topology does not fail the run.
        }
    }

    /// <summary>
    /// A small pool of RabbitMQ channels so concurrent publishes stay
    /// thread-safe (an IModel is single-threaded) without per-publish churn.
    /// Each publish rents a channel, creates properties on it, publishes, and
    /// returns the channel to the pool.
    /// </summary>
    private sealed class RabbitPublishPool : IDisposable
    {
        private readonly IConnection _connection;
        private readonly ConcurrentQueue<IModel> _channels = new();
        private readonly SemaphoreSlim _semaphore;

        public RabbitPublishPool(IConnection connection, int size)
        {
            _connection = connection;
            _semaphore = new SemaphoreSlim(size, size);
            for (var i = 0; i < size; i++)
            {
                _channels.Enqueue(connection.CreateModel());
            }
        }

        public async Task PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            string messageId,
            CancellationToken cancellationToken)
        {
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (!_channels.TryDequeue(out var channel))
                {
                    channel = _connection.CreateModel();
                }
                var properties = channel.CreateBasicProperties();
                properties.ContentType = "application/json";
                properties.MessageId = messageId;
                channel.BasicPublish(
                    exchange, routingKey, mandatory: false,
                    basicProperties: properties, body: body);
                _channels.Enqueue(channel);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public void Dispose()
        {
            while (_channels.TryDequeue(out var channel))
            {
                try { channel.Dispose(); } catch { /* ignore */ }
            }
            _semaphore.Dispose();
        }
    }
}