using System.Diagnostics;
using KubeJob;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Storage.PostgreSQL.Data;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace KubeJob.Benchmark;

/// <summary>
/// V3 end-to-end benchmark. PostgresManaged and BrokerNative are intentionally
/// built as different data planes while sharing the same Job handler. The
/// BrokerNative completion tracker is process-local so PostgreSQL is never read
/// merely to discover completion.
/// </summary>
public sealed class PipelineBenchmark
{
    private readonly BenchmarkOptions _opts;

    public PipelineBenchmark(BenchmarkOptions opts) => _opts = opts;

    public async Task<IReadOnlyList<ScenarioResult>> RunScenarioAsync(BenchScenario scenario)
    {
        if (_opts.RuntimeMode == QueueRuntimeMode.BrokerNative
            && scenario != BenchScenario.Parallel)
        {
            throw new NotSupportedException(
                "BrokerNative benchmark currently supports Parallel only; " +
                "transport-native partition ordering is not enabled in this benchmark yet.");
        }

        return new[] { await RunScenarioSingleAsync(scenario) };
    }

    private async Task<ScenarioResult> RunScenarioSingleAsync(BenchScenario scenario)
    {
        var suffix = Guid.NewGuid().ToString("N");
        var queue = scenario.QueueName();
        var warmupQueue = $"{queue}.warmup";
        var rabbitOptions = CreateRabbitOptions(suffix);
        var runSw = Stopwatch.StartNew();
        var (benchConnStr, dbName) = await CreateFreshDatabaseAsync();

        IHost? host = null;
        MetricsSampler? sampler = null;
        try
        {
            if (_opts.RuntimeMode == QueueRuntimeMode.PostgresManaged)
            {
                InitializeSchema(benchConnStr);
            }

            host = BuildHost(benchConnStr + ";No Reset On Close=true", scenario, queue, warmupQueue, rabbitOptions);
            await host.StartAsync();

            if (_opts.RuntimeMode == QueueRuntimeMode.BrokerNative)
            {
                await WaitForRabbitReadyAsync(rabbitOptions, new[] { queue, warmupQueue });
            }

            var tracker = host.Services.GetRequiredService<BenchCompletionTracker>();
            if (_opts.Warmup > 0)
            {
                tracker.Begin(_opts.Warmup);
                await SubmitAsync(host, scenario, warmupQueue, _opts.Warmup);
                var warmup = await tracker.WaitAsync(TimeSpan.FromSeconds(60));
                if (warmup.Completed != _opts.Warmup)
                {
                    throw new TimeoutException(
                        $"Benchmark warmup completed {warmup.Completed}/{_opts.Warmup} jobs.");
                }
            }

            var metricsQueues = _opts.RuntimeMode == QueueRuntimeMode.BrokerNative
                ? new[] { rabbitOptions.GetQueueName(queue) }
                : Array.Empty<string>();
            sampler = new MetricsSampler(
                benchConnStr,
                _opts.RabbitMqManagementUri,
                _opts.RabbitMqUser,
                _opts.RabbitMqPassword,
                metricsQueues,
                _opts.CpuSamplingEnabled ? _opts.PostgresContainerName : null,
                TimeSpan.FromMilliseconds(_opts.MetricsIntervalMs));

            tracker.Begin(_opts.JobCount);
            var submitSw = Stopwatch.StartNew();
            await SubmitAsync(host, scenario, queue, _opts.JobCount);
            submitSw.Stop();

            var e2eSw = Stopwatch.StartNew();
            var completion = await tracker.WaitAsync(TimeSpan.FromSeconds(_opts.RunTimeoutSeconds));
            e2eSw.Stop();
            runSw.Stop();

            var succeeded = completion.Completed;
            var incomplete = Math.Max(0, _opts.JobCount - completion.Completed);
            var ingestTps = submitSw.Elapsed.TotalSeconds > 0
                ? _opts.JobCount / submitSw.Elapsed.TotalSeconds
                : 0;
            var e2eSeconds = submitSw.Elapsed.TotalSeconds + e2eSw.Elapsed.TotalSeconds;
            var e2eTps = e2eSeconds > 0 ? succeeded / e2eSeconds : 0;

            return new ScenarioResult(
                scenario,
                _opts.RuntimeMode,
                _opts.JobCount,
                succeeded,
                Failed: 0,
                incomplete,
                ingestTps,
                e2eTps,
                Percentiles.Compute(completion.LatencySamplesMs),
                sampler.Snapshot(),
                runSw.Elapsed);
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
                        // Best effort during benchmark cleanup.
                    }
                }
            }

            if (_opts.RuntimeMode == QueueRuntimeMode.BrokerNative)
            {
                DeleteBrokerTopology(rabbitOptions, new[] { queue, warmupQueue });
            }

            await DropDatabaseAsync(dbName);
        }
    }

    private IHost BuildHost(
        string benchConnStr,
        BenchScenario scenario,
        string queue,
        string warmupQueue,
        RabbitMqBrokerNativeOptions rabbitOptions)
    {
        var builder = new HostBuilder()
            .ConfigureLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Warning))
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddSingleton(new BenchJobOptions { WorkMs = _opts.JobWorkMs });
                services.AddSingleton<BenchCompletionTracker>();
                services.AddKubeJobHandler<NoopBenchJob, BenchPayload>(NoopBenchJob.JobKey);

                if (_opts.RuntimeMode == QueueRuntimeMode.PostgresManaged)
                {
                    var backgroundPool = Math.Max(16, _opts.OutboxPublishConcurrency + 16);
                    var businessPool = Math.Max(48, _opts.SubmitterConcurrency + _opts.WorkerMaxConcurrency + 48);
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
                            worker.WorkerId = $"bench-managed-{Guid.NewGuid():N}";
                            worker.BuildId = "bench";
                            worker.ConsumerGroup = "bench";
                            worker.Queues = new List<string> { queue, warmupQueue };
                            worker.MaxConcurrentJobs = _opts.WorkerMaxConcurrency;
                            worker.ClaimBatchSize = Math.Min(256, _opts.WorkerMaxConcurrency);
                            worker.EmptyPollDelay = TimeSpan.FromMilliseconds(5);
                            worker.HeartbeatInterval = TimeSpan.FromSeconds(1);
                            worker.LeaseRenewalInterval = TimeSpan.FromSeconds(1);
                            worker.DrainTimeout = TimeSpan.FromSeconds(5);
                        });

                    services.ConfigureKubeJobQueueRouting(routing =>
                    {
                        routing.Queues[queue] = new KubeJob.ControlPlane.Runtime.QueueDefinition
                        {
                            OrderingMode = scenario.OrderingMode(),
                            ConsumerGroup = "bench"
                        };
                        routing.Queues[warmupQueue] = new KubeJob.ControlPlane.Runtime.QueueDefinition
                        {
                            OrderingMode = ExecutionOrderingMode.Parallel,
                            ConsumerGroup = "bench"
                        };
                    });
                    services.Configure<JobRuntimeOptions>(runtime =>
                    {
                        runtime.OutboxPublishConcurrency = _opts.OutboxPublishConcurrency;
                        runtime.OutboxBatchSize = _opts.OutboxBatchSize;
                        runtime.OutboxPollInterval = TimeSpan.FromMilliseconds(_opts.OutboxPollIntervalMs);
                        runtime.CompletionBatchSize = Math.Min(256, _opts.WorkerMaxConcurrency);
                        runtime.CompletionBatcherShardCount = 8;
                        runtime.CompletionFlushInterval = TimeSpan.FromMilliseconds(2);
                        runtime.LeaseDuration = TimeSpan.FromMinutes(2);
                        runtime.MaxClaimBatchSize = 1024;
                    });
                }
                else
                {
                    services.AddKubeJobServer();
                    services.ConfigureKubeJobQueueRuntimes(runtime =>
                    {
                        runtime.Queues[queue] = new QueueRuntimeRoute
                        {
                            Mode = QueueRuntimeMode.BrokerNative,
                            TransportId = RabbitMqBrokerNativePublisher.Id
                        };
                        runtime.Queues[warmupQueue] = new QueueRuntimeRoute
                        {
                            Mode = QueueRuntimeMode.BrokerNative,
                            TransportId = RabbitMqBrokerNativePublisher.Id
                        };
                    });
                    services.AddKubeJobBrokerNativeWorker(worker =>
                    {
                        worker.WorkerId = $"bench-broker-{Guid.NewGuid():N}";
                        worker.BuildId = "bench";
                        worker.Queues = new List<string> { queue, warmupQueue };
                        worker.MaxConcurrentJobs = _opts.WorkerMaxConcurrency;
                    });
                    services.AddRabbitMqKubeJobBrokerNativeConsumer(options =>
                        CopyRabbitOptions(rabbitOptions, options));
                }
            });

        return builder.Build();
    }

    private async Task SubmitAsync(IHost host, BenchScenario scenario, string queue, int count)
    {
        var client = host.Services.GetRequiredService<IJobClient>();
        var parallel = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, _opts.SubmitterConcurrency)
        };

        await Parallel.ForEachAsync(Enumerable.Range(0, count), parallel, async (index, cancellationToken) =>
        {
            var concurrencyKey = _opts.RuntimeMode == QueueRuntimeMode.PostgresManaged
                ? scenario.ConcurrencyKey(index, _opts.HotKeyCardinality, _opts.UniformKeyCardinality)
                : null;
            var payload = new BenchPayload(index, DateTimeOffset.UtcNow.UtcTicks);
            await client.EnqueueAsync(
                NoopBenchJob.JobKey,
                payload,
                new JobEnqueueOptions
                {
                    Queue = queue,
                    ConcurrencyKey = concurrencyKey,
                    MaxAttempts = 1,
                    Timeout = TimeSpan.FromMinutes(2)
                },
                cancellationToken);
        });
    }

    private RabbitMqBrokerNativeOptions CreateRabbitOptions(string suffix) => new()
    {
        ConnectionString = _opts.RabbitMqConnectionString,
        ExchangeName = $"kubejob.bench.jobs.{suffix}",
        QueuePrefix = $"kubejob.bench.{suffix}",
        PrefetchCount = checked((ushort)Math.Min(ushort.MaxValue, Math.Max(1, _opts.PrefetchCount))),
        ConsumerDispatchConcurrency = checked((ushort)Math.Min(256, Math.Max(1, _opts.WorkerMaxConcurrency))),
        RetryDelay = TimeSpan.FromSeconds(1),
        ReconnectDelay = TimeSpan.FromMilliseconds(250),
        PublisherConfirmTimeout = TimeSpan.FromSeconds(10)
    };

    private static void CopyRabbitOptions(
        RabbitMqBrokerNativeOptions source,
        RabbitMqBrokerNativeOptions target)
    {
        target.ConnectionString = source.ConnectionString;
        target.ExchangeName = source.ExchangeName;
        target.QueuePrefix = source.QueuePrefix;
        target.PrefetchCount = source.PrefetchCount;
        target.ConsumerDispatchConcurrency = source.ConsumerDispatchConcurrency;
        target.RetryDelay = source.RetryDelay;
        target.ReconnectDelay = source.ReconnectDelay;
        target.PublisherConfirmTimeout = source.PublisherConfirmTimeout;
    }

    private async Task WaitForRabbitReadyAsync(
        RabbitMqBrokerNativeOptions options,
        IEnumerable<string> logicalQueues)
    {
        using var connection = OpenRabbitConnection();
        foreach (var logicalQueue in logicalQueues)
        {
            var physicalQueue = options.GetQueueName(logicalQueue);
            var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
            while (DateTimeOffset.UtcNow < deadline)
            {
                try
                {
                    using var channel = connection.CreateModel();
                    if (channel.ConsumerCount(physicalQueue) >= 1)
                    {
                        break;
                    }
                }
                catch (OperationInterruptedException exception)
                    when (exception.ShutdownReason?.ReplyCode == 404)
                {
                    // Consumer topology has not been declared yet.
                }

                await Task.Delay(100);
            }

            using var verify = connection.CreateModel();
            if (verify.ConsumerCount(physicalQueue) < 1)
            {
                throw new TimeoutException($"Timed out waiting for RabbitMQ consumer on '{physicalQueue}'.");
            }
        }
    }

    private IConnection OpenRabbitConnection() =>
        new ConnectionFactory
        {
            Uri = new Uri(_opts.RabbitMqConnectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = false
        }.CreateConnection("kubejob-v3-benchmark");

    private void DeleteBrokerTopology(
        RabbitMqBrokerNativeOptions options,
        IEnumerable<string> logicalQueues)
    {
        try
        {
            using var connection = OpenRabbitConnection();
            using var channel = connection.CreateModel();
            foreach (var logicalQueue in logicalQueues)
            {
                TryDeleteQueue(channel, options.GetQueueName(logicalQueue));
            }
            TryDeleteQueue(channel, options.GetRetryQueueName());
            TryDeleteQueue(channel, options.GetDeadLetterQueueName());
            TryDeleteExchange(channel, options.GetRetryExchangeName());
            TryDeleteExchange(channel, options.GetDeadLetterExchangeName());
            TryDeleteExchange(channel, options.ExchangeName);
        }
        catch
        {
            // Best effort; every benchmark run uses unique topology.
        }
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

    private void InitializeSchema(string benchConnStr)
    {
        var noPool = new NpgsqlConnectionStringBuilder(benchConnStr) { Pooling = false }.ConnectionString;
        new DbInitializer(noPool).Initialize();
    }

    private async Task<(string BenchConnectionString, string DatabaseName)> CreateFreshDatabaseAsync()
    {
        var adminBuilder = new NpgsqlConnectionStringBuilder(_opts.PostgresConnectionString)
        {
            Database = "postgres",
            Pooling = false
        };
        var databaseName = "kubejob_bench_" + Guid.NewGuid().ToString("N");
        await using (var admin = new NpgsqlConnection(adminBuilder.ConnectionString))
        {
            await admin.OpenAsync();
            await using var create = admin.CreateCommand();
            create.CommandText = $"CREATE DATABASE \"{databaseName}\"";
            await create.ExecuteNonQueryAsync();
        }

        var benchmarkBuilder = new NpgsqlConnectionStringBuilder(_opts.PostgresConnectionString)
        {
            Database = databaseName,
            Pooling = true
        };
        return (benchmarkBuilder.ConnectionString, databaseName);
    }

    private async Task DropDatabaseAsync(string databaseName)
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
            terminate.Parameters.AddWithValue("db", databaseName);
            await terminate.ExecuteNonQueryAsync();

            await using var drop = admin.CreateCommand();
            drop.CommandText = $"DROP DATABASE IF EXISTS \"{databaseName}\"";
            await drop.ExecuteNonQueryAsync();
        }
        catch
        {
            // Cleanup failure must not hide benchmark results.
        }
    }
}
