using System.Collections.Concurrent;
using System.Diagnostics;
using KubeJob;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Data;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using RabbitMQ.Client;

namespace KubeJob.Benchmark;

/// <summary>
/// Drives the current PostgresManaged runtime end to end. Typed submissions use
/// the production client directly; the optional ingress mode publishes business
/// messages to RabbitMQ, which the ingress adapter converts into managed Runs.
/// Execution itself remains claim/lease/complete in PostgreSQL in both modes.
/// </summary>
public sealed class PipelineBenchmark
{
    private const string IngressPrefix = "kubejob.bench.ingress";
    private readonly BenchmarkOptions _opts;

    public PipelineBenchmark(BenchmarkOptions opts)
    {
        _opts = opts ?? throw new ArgumentNullException(nameof(opts));
    }

    public async Task<IReadOnlyList<ScenarioResult>> RunScenarioAsync(BenchScenario scenario)
    {
        var results = new List<ScenarioResult>(_opts.LaneCountSweep.Count);
        foreach (var laneCount in _opts.LaneCountSweep)
        {
            results.Add(await RunScenarioSingleAsync(scenario, Math.Max(1, laneCount)));
        }

        return results;
    }

    private async Task<ScenarioResult> RunScenarioSingleAsync(BenchScenario scenario, int laneCount)
    {
        var group = $"bench-{scenario.Label().ToLowerInvariant()}-{Guid.NewGuid():N}";
        var topology = BuildTopology(group);
        var scenarioSw = Stopwatch.StartNew();
        var (benchConnStr, dbName) = await CreateFreshDatabaseAsync();
        await SetSynchronousCommitOffAsync(benchConnStr);

        IHost? host = null;
        MetricsSampler? sampler = null;
        IConnection? rabbitConnection = null;
        try
        {
            host = BuildHost(
                benchConnStr + ";No Reset On Close=true",
                scenario,
                group,
                laneCount,
                topology);
            InitializeSchema(benchConnStr);
            await host.StartAsync();

            if (_opts.SubmissionMode == SubmissionMode.Ingress)
            {
                rabbitConnection = OpenRabbitConnection();
                await WaitForIngressReadyAsync(rabbitConnection, topology.IngressQueue!);
            }

            if (_opts.Warmup > 0)
            {
                var savedCount = _opts.JobCount;
                _opts.JobCount = _opts.Warmup;
                try
                {
                    var warmupQueue = WarmupQueue(scenario.QueueName());
                    await SubmitAsync(host, scenario, warmupQueue, topology, rabbitConnection);
                    await WaitForCompletionAsync(
                        host,
                        warmupQueue,
                        _opts.Warmup,
                        TimeSpan.FromSeconds(60));
                }
                finally
                {
                    _opts.JobCount = savedCount;
                }
            }

            sampler = new MetricsSampler(
                benchConnStr,
                _opts.RabbitMqManagementUri,
                _opts.RabbitMqUser,
                _opts.RabbitMqPassword,
                topology.MetricsQueues,
                _opts.CpuSamplingEnabled ? _opts.PostgresContainerName : null,
                TimeSpan.FromMilliseconds(Math.Max(1, _opts.MetricsIntervalMs)));

            var submitSw = Stopwatch.StartNew();
            var e2eWallSw = Stopwatch.StartNew();
            await SubmitAsync(host, scenario, scenario.QueueName(), topology, rabbitConnection);
            submitSw.Stop();

            var completion = await WaitForCompletionAsync(
                host,
                scenario.QueueName(),
                _opts.JobCount);
            e2eWallSw.Stop();
            scenarioSw.Stop();

            var metrics = sampler.Snapshot();
            return BuildResult(
                scenario,
                laneCount,
                completion,
                submitSw,
                e2eWallSw,
                scenarioSw,
                metrics);
        }
        finally
        {
            if (sampler is not null)
            {
                await sampler.DisposeAsync();
            }

            if (host is not null)
            {
                using (host)
                {
                    try
                    {
                        using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                        await host.StopAsync(stopCts.Token);
                    }
                    catch
                    {
                        // Best effort; the per-run database is still isolated.
                    }
                }
            }

            rabbitConnection?.Dispose();
            DeleteIngressTopology(topology);
            await DropDatabaseAsync(dbName);
        }
    }

    private sealed record BenchTopology(
        string Group,
        string? IngressExchange,
        string? IngressQueue)
    {
        public IReadOnlyList<string> MetricsQueues =>
            IngressQueue is null ? Array.Empty<string>() : new[] { IngressQueue };
    }

    private BenchTopology BuildTopology(string group)
    {
        if (_opts.SubmissionMode != SubmissionMode.Ingress)
        {
            return new BenchTopology(group, null, null);
        }

        return new BenchTopology(
            group,
            $"{IngressPrefix}.{group}.exchange",
            $"{IngressPrefix}.{group}.queue");
    }

    private IHost BuildHost(
        string connectionString,
        BenchScenario scenario,
        string group,
        int laneCount,
        BenchTopology topology)
    {
        var queue = scenario.QueueName();
        var warmupQueue = WarmupQueue(queue);
        var lane = $"bench-lane-{laneCount}";
        var workerPool = Math.Max(16, _opts.OutboxPublishConcurrency + 16);
        var businessPool = Math.Max(
            48,
            _opts.SubmitterConcurrency + _opts.WorkerMaxConcurrency + 48);

        return new HostBuilder()
            .ConfigureLogging(builder => builder
                .AddConsole()
                .SetMinimumLevel(LogLevel.Warning))
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddKubeJob(
                    configureServer: server => server.UsePostgreSql(
                        connectionString,
                        storage =>
                        {
                            storage.BusinessPoolSize = businessPool;
                            storage.BackgroundPoolSize = workerPool;
                        }),
                    configureWorker: worker =>
                    {
                        worker.WorkerId = $"bench-worker-{group}";
                        worker.BuildId = "bench";
                        worker.ConsumerGroup = group;
                        worker.ExecutionLane = lane;
                        worker.Queues = new List<string> { queue, warmupQueue };
                        worker.MaxConcurrentJobs = Math.Max(1, _opts.WorkerMaxConcurrency);
                        worker.ClaimBatchSize = Math.Min(1024, Math.Max(1, _opts.WorkerMaxConcurrency));
                        worker.EmptyPollDelay = TimeSpan.FromMilliseconds(5);
                        worker.HeartbeatInterval = TimeSpan.FromSeconds(1);
                        worker.LeaseRenewalInterval = TimeSpan.FromSeconds(1);
                        worker.DrainTimeout = TimeSpan.FromSeconds(5);
                    });

                services.ConfigureKubeJobQueueRouting(routing =>
                {
                    routing.Defaults.ExecutionLane = lane;
                    routing.Defaults.ConsumerGroup = group;
                    routing.Defaults.OrderingMode = ExecutionOrderingMode.Parallel;
                    routing.Queues[queue] = new QueueDefinition
                    {
                        ExecutionLane = lane,
                        ConsumerGroup = group,
                        OrderingMode = scenario.OrderingMode()
                    };
                    routing.Queues[warmupQueue] = new QueueDefinition
                    {
                        ExecutionLane = lane,
                        ConsumerGroup = group,
                        OrderingMode = ExecutionOrderingMode.Parallel
                    };
                });

                if (_opts.SubmissionMode == SubmissionMode.Ingress)
                {
                    services.AddRabbitMqKubeJobIngress(options =>
                    {
                        options.ConnectionString = _opts.RabbitMqConnectionString;
                        options.ExchangeName = topology.IngressExchange!;
                        options.QueueName = topology.IngressQueue!;
                        options.RoutingKey = "#";
                        options.Source = "kubejob-bench";
                        options.AllowNoDeadLetterExchange = true;
                        options.PrefetchCount = (ushort)Math.Clamp(_opts.IngressPrefetch, 1, ushort.MaxValue);
                        options.SubmissionBatchSize = Math.Max(1, _opts.IngressBatchSize);
                        options.SubmissionBatchWait = TimeSpan.FromMilliseconds(
                            Math.Clamp(_opts.IngressBatchWaitMs, 1, 1000));
                    });
                }

                services.AddSingleton(new BenchJobOptions { WorkMs = _opts.JobWorkMs });
                services.AddKubeJobHandler<NoopBenchJob, BenchPayload>(NoopBenchJob.JobKey);
                services.Configure<JobRuntimeOptions>(runtime =>
                {
                    runtime.OutboxPublishConcurrency = Math.Max(1, _opts.OutboxPublishConcurrency);
                    runtime.OutboxBatchSize = Math.Max(1, _opts.OutboxBatchSize);
                    runtime.OutboxPollInterval = TimeSpan.FromMilliseconds(
                        Math.Max(1, _opts.OutboxPollIntervalMs));
                    runtime.CompletionBatchSize = Math.Min(
                        256,
                        Math.Max(1, _opts.WorkerMaxConcurrency));
                    runtime.CompletionBatcherShardCount = 8;
                    runtime.CompletionFlushInterval = TimeSpan.FromMilliseconds(
                        Math.Max(1, _opts.CompletionFlushIntervalMs));
                    runtime.LeaseDuration = TimeSpan.FromMinutes(2);
                    runtime.MaxClaimBatchSize = 1024;
                });
            })
            .Build();
    }

    private void InitializeSchema(string connectionString)
    {
        var noPool = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false };
        new DbInitializer(noPool.ConnectionString).Initialize();
    }

    private IConnection OpenRabbitConnection() =>
        new ConnectionFactory
        {
            Uri = new Uri(_opts.RabbitMqConnectionString, UriKind.Absolute)
        }.CreateConnection("kubejob-bench");

    private static async Task WaitForIngressReadyAsync(
        IConnection connection,
        string queue)
    {
        using var channel = connection.CreateModel();
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (DateTimeOffset.UtcNow < deadline)
        {
            try
            {
                if (channel.ConsumerCount(queue) > 0)
                {
                    return;
                }
            }
            catch (RabbitMQ.Client.Exceptions.OperationInterruptedException)
            {
                // The service has not declared the queue yet.
            }

            await Task.Delay(100);
        }

        throw new TimeoutException($"Timed out waiting for RabbitMQ ingress queue '{queue}'.");
    }

    private Task SubmitAsync(
        IHost host,
        BenchScenario scenario,
        string queue,
        BenchTopology topology,
        IConnection? connection) =>
        _opts.SubmissionMode == SubmissionMode.Ingress
            ? SubmitIngressAsync(
                scenario,
                queue,
                topology,
                connection ?? throw new InvalidOperationException("Ingress requires RabbitMQ."))
            : SubmitTypedAsync(host, scenario, queue);

    private async Task SubmitTypedAsync(
        IHost host,
        BenchScenario scenario,
        string queue)
    {
        var client = host.Services.GetRequiredService<IJobClient>();
        const int batchSize = 100;
        for (var offset = 0; offset < _opts.JobCount; offset += batchSize)
        {
            var end = Math.Min(offset + batchSize, _opts.JobCount);
            var items = new List<(BenchPayload Payload, JobEnqueueOptions? Options)>(end - offset);
            for (var index = offset; index < end; index++)
            {
                items.Add((
                    new BenchPayload(index),
                    new JobEnqueueOptions
                    {
                        Queue = queue,
                        ConcurrencyKey = scenario.ConcurrencyKey(
                            index,
                            _opts.HotKeyCardinality,
                            _opts.UniformKeyCardinality),
                        MaxAttempts = 1,
                        Timeout = TimeSpan.FromMinutes(2)
                    }));
            }

            await client.EnqueueBatchAsync(NoopBenchJob.JobKey, items);
        }
    }

    private async Task SubmitIngressAsync(
        BenchScenario scenario,
        string queue,
        BenchTopology topology,
        IConnection connection)
    {
        using var pool = new RabbitPublishPool(
            connection,
            Math.Max(1, _opts.SubmitterConcurrency));
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, _opts.SubmitterConcurrency)
        };

        await Parallel.ForEachAsync(
            Enumerable.Range(0, _opts.JobCount),
            options,
            async (index, cancellationToken) =>
            {
                var envelope = new RabbitMqJobIngressEnvelope(
                    MessageId: $"{topology.Group}-{index}",
                    JobKey: NoopBenchJob.BenchJobKeyString,
                    PayloadJson: $"{{\"Value\":{index}}}",
                    Queue: queue,
                    ConcurrencyKey: scenario.ConcurrencyKey(
                        index,
                        _opts.HotKeyCardinality,
                        _opts.UniformKeyCardinality),
                    MaxAttempts: 1,
                    TimeoutSeconds: 120);
                var body = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(envelope);
                await pool.PublishAsync(
                    topology.IngressExchange!,
                    "bench.noop",
                    body,
                    envelope.MessageId,
                    cancellationToken);
            });
    }

    private static string WarmupQueue(string measuredQueue) => $"{measuredQueue}.warmup";

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
        if (expected == 0)
        {
            return new CompletionResult(0, 0, 0, 0, 0, Array.Empty<double>());
        }

        var dashboard = host.Services.GetRequiredService<IJobRuntimeDashboardStore>();
        var collected = new ConcurrentDictionary<string, DashboardRunSummary>();
        var deadline = DateTimeOffset.UtcNow +
            (timeout ?? TimeSpan.FromSeconds(Math.Max(1, _opts.RunTimeoutSeconds)));

        while (collected.Count < expected && DateTimeOffset.UtcNow < deadline)
        {
            await PageAndCollectAsync(dashboard, queue, collected);
            if (collected.Count >= expected)
            {
                break;
            }

            await Task.Delay(Math.Max(1, _opts.PollIntervalMs));
        }

        return BuildCompletion(expected, collected);
    }

    private static async Task PageAndCollectAsync(
        IJobRuntimeDashboardStore dashboard,
        string queue,
        ConcurrentDictionary<string, DashboardRunSummary> collected)
    {
        var page = 1;
        while (true)
        {
            var result = await dashboard.GetRunsAsync(
                new DashboardRunQuery(Page: page, PageSize: 100, Queue: queue),
                CancellationToken.None);
            foreach (var item in result.Items)
            {
                if (IsTerminal(item.Phase))
                {
                    collected[item.Id] = item;
                }
            }

            var pages = result.TotalCount == 0
                ? 1
                : (int)Math.Ceiling(result.TotalCount / 100.0);
            if (page >= pages)
            {
                break;
            }

            page++;
        }
    }

    private static bool IsTerminal(JobPhase phase) =>
        phase is JobPhase.Succeeded or JobPhase.Failed or JobPhase.Canceled or JobPhase.Dead;

    private static CompletionResult BuildCompletion(
        int expected,
        ConcurrentDictionary<string, DashboardRunSummary> collected)
    {
        var succeeded = 0;
        var failed = 0;
        var canceledOrDead = 0;
        var minCreated = long.MaxValue;
        var maxCompleted = long.MinValue;
        var latencies = new List<double>(collected.Count);

        foreach (var run in collected.Values)
        {
            if (run.Phase == JobPhase.Succeeded)
            {
                succeeded++;
            }
            else if (run.Phase == JobPhase.Failed)
            {
                failed++;
            }
            else
            {
                canceledOrDead++;
            }

            minCreated = Math.Min(minCreated, run.CreatedAt.UtcTicks);
            if (run.CompletedAt is { } completed)
            {
                latencies.Add((completed - run.CreatedAt).TotalMilliseconds);
                maxCompleted = Math.Max(maxCompleted, completed.UtcTicks);
            }
        }

        canceledOrDead += Math.Max(0, expected - collected.Count);
        return new CompletionResult(
            succeeded,
            failed,
            canceledOrDead,
            minCreated == long.MaxValue ? 0 : minCreated,
            maxCompleted == long.MinValue ? 0 : maxCompleted,
            latencies.ToArray());
    }

    private ScenarioResult BuildResult(
        BenchScenario scenario,
        int laneCount,
        CompletionResult completion,
        Stopwatch submitSw,
        Stopwatch e2eWallSw,
        Stopwatch scenarioSw,
        MetricSamples metrics)
    {
        var ingestTps = submitSw.Elapsed.TotalSeconds > 0
            ? _opts.JobCount / submitSw.Elapsed.TotalSeconds
            : 0;
        var e2eTps = 0.0;
        if (completion.MaxCompletedAtTicks > completion.MinCreatedAtTicks
            && completion.MinCreatedAtTicks > 0)
        {
            var span = TimeSpan.FromTicks(
                completion.MaxCompletedAtTicks - completion.MinCreatedAtTicks);
            e2eTps = completion.Succeeded / span.TotalSeconds;
        }

        var wallClockE2eTps = e2eWallSw.Elapsed.TotalSeconds > 0
            ? _opts.JobCount / e2eWallSw.Elapsed.TotalSeconds
            : 0;

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
            scenarioSw.Elapsed)
        {
            Mode = _opts.SubmissionMode.ToString(),
            LaneCount = laneCount
        };
    }

    private async Task<(string BenchConnectionString, string DatabaseName)> CreateFreshDatabaseAsync()
    {
        var adminOptions = new NpgsqlConnectionStringBuilder(_opts.PostgresConnectionString)
        {
            Database = "postgres",
            Pooling = false
        };
        var databaseName = "kubejob_bench_" + Guid.NewGuid().ToString("N");
        await using (var admin = new NpgsqlConnection(adminOptions.ConnectionString))
        {
            await admin.OpenAsync();
            await using var command = admin.CreateCommand();
            command.CommandText = $"CREATE DATABASE \"{databaseName}\"";
            await command.ExecuteNonQueryAsync();
        }

        var benchmarkOptions = new NpgsqlConnectionStringBuilder(_opts.PostgresConnectionString)
        {
            Database = databaseName,
            Pooling = true
        };
        return (benchmarkOptions.ConnectionString, databaseName);
    }

    private static async Task SetSynchronousCommitOffAsync(string connectionString)
    {
        var options = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false };
        await using var connection = new NpgsqlConnection(options.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"ALTER DATABASE \"{connection.Database}\" SET synchronous_commit = off";
        await command.ExecuteNonQueryAsync();
    }

    private async Task DropDatabaseAsync(string databaseName)
    {
        var adminOptions = new NpgsqlConnectionStringBuilder(_opts.PostgresConnectionString)
        {
            Database = "postgres",
            Pooling = false
        };
        try
        {
            await using var admin = new NpgsqlConnection(adminOptions.ConnectionString);
            await admin.OpenAsync();
            await using var terminate = admin.CreateCommand();
            terminate.CommandText =
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
                "WHERE datname = @db AND pid <> pg_backend_pid();";
            terminate.Parameters.AddWithValue("db", databaseName);
            await terminate.ExecuteNonQueryAsync();

            await using var drop = admin.CreateCommand();
            drop.CommandText = $"DROP DATABASE IF EXISTS \"{databaseName}\"";
            await drop.ExecuteNonQueryAsync();
        }
        catch
        {
            // Best effort cleanup must not mask a benchmark result.
        }
    }

    private void DeleteIngressTopology(BenchTopology topology)
    {
        if (topology.IngressQueue is null || topology.IngressExchange is null)
        {
            return;
        }

        try
        {
            using var connection = OpenRabbitConnection();
            using var channel = connection.CreateModel();
            channel.QueueDelete(topology.IngressQueue, ifUnused: false, ifEmpty: false);
            channel.ExchangeDelete(topology.IngressExchange);
        }
        catch
        {
            // Best effort broker cleanup.
        }
    }

    private sealed class RabbitPublishPool : IDisposable
    {
        private readonly IConnection _connection;
        private readonly ConcurrentQueue<IModel> _channels = new();
        private readonly SemaphoreSlim _semaphore;

        public RabbitPublishPool(IConnection connection, int size)
        {
            _connection = connection;
            _semaphore = new SemaphoreSlim(size, size);
            for (var index = 0; index < size; index++)
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

                try
                {
                    var properties = channel.CreateBasicProperties();
                    properties.ContentType = "application/json";
                    properties.MessageId = messageId;
                    channel.BasicPublish(
                        exchange,
                        routingKey,
                        mandatory: false,
                        basicProperties: properties,
                        body: body.ToArray());
                }
                finally
                {
                    _channels.Enqueue(channel);
                }
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
                try
                {
                    channel.Dispose();
                }
                catch
                {
                    // Best effort channel cleanup.
                }
            }

            _semaphore.Dispose();
        }
    }
}
