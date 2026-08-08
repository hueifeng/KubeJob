using System.Diagnostics;
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
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace KubeJob.Benchmark;

/// <summary>
/// RabbitMQ-authoritative BrokerNative throughput baseline. Unlike the managed
/// benchmark, this host never configures PostgreSQL storage or a managed worker
/// runtime. The hot path is IJobClient -> RabbitMQ -> BrokerNative worker ->
/// handler -> ACK.
/// </summary>
public sealed class BrokerNativePipelineBenchmark
{
    private const string LogicalQueue = "bench.broker-native";
    private static readonly JobKey<BrokerNativeBenchPayload> JobKey = new("bench.broker-native.noop");

    private readonly BenchmarkOptions _options;

    public BrokerNativePipelineBenchmark(BenchmarkOptions options)
    {
        _options = options;
    }

    public async Task<BrokerNativeBenchmarkResult> RunAsync()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var transport = new RabbitMqBrokerNativeOptions
        {
            ConnectionString = _options.RabbitMqConnectionString,
            ExchangeName = $"kubejob.bench.native.{suffix}",
            QueuePrefix = $"kubejob.bench.native.{suffix}",
            PrefetchCount = (ushort)Math.Clamp(_options.WorkerMaxConcurrency * 2, 1, ushort.MaxValue),
            RetryDelay = TimeSpan.FromSeconds(1),
            ReconnectDelay = TimeSpan.FromMilliseconds(100),
            PublisherConfirmTimeout = TimeSpan.FromSeconds(10)
        };
        transport.Validate();

        var probe = new BrokerNativeBenchProbe(_options.Warmup, _options.JobCount);
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddSingleton(probe);
                services.AddSingleton(new BrokerNativeBenchJobOptions { WorkMs = _options.JobWorkMs });
                services.AddKubeJobServer();
                services.ConfigureKubeJobQueueRuntimes(runtime =>
                {
                    runtime.Queues[LogicalQueue] = new QueueRuntimeRoute
                    {
                        Mode = QueueRuntimeMode.BrokerNative,
                        TransportId = RabbitMqBrokerNativePublisher.Id
                    };
                });
                services.AddKubeJobHandler<BrokerNativeBenchJob, BrokerNativeBenchPayload>(JobKey);
                services.AddKubeJobBrokerNativeWorker(worker =>
                {
                    worker.WorkerId = $"bench-native-{suffix}";
                    worker.BuildId = "benchmark";
                    worker.Queues = new List<string> { LogicalQueue };
                    worker.MaxConcurrentJobs = _options.WorkerMaxConcurrency;
                });
                services.AddRabbitMqKubeJobBrokerNativeConsumer(rabbit =>
                {
                    rabbit.ConnectionString = transport.ConnectionString;
                    rabbit.ExchangeName = transport.ExchangeName;
                    rabbit.QueuePrefix = transport.QueuePrefix;
                    rabbit.PrefetchCount = transport.PrefetchCount;
                    rabbit.RetryDelay = transport.RetryDelay;
                    rabbit.ReconnectDelay = transport.ReconnectDelay;
                    rabbit.PublisherConfirmTimeout = transport.PublisherConfirmTimeout;
                });
            })
            .Build();

        // Architectural guard: this benchmark must not accidentally become a
        // PostgreSQL-backed pipeline while still being labeled BrokerNative.
        if (host.Services.GetService<NpgsqlDataSource>() is not null)
        {
            throw new InvalidOperationException(
                "BrokerNative benchmark unexpectedly registered PostgreSQL storage.");
        }

        await host.StartAsync();
        try
        {
            await WaitForConsumerAsync(
                transport.ConnectionString,
                transport.GetQueueName(LogicalQueue),
                TimeSpan.FromSeconds(20));

            var client = host.Services.GetRequiredService<IJobClient>();

            if (_options.Warmup > 0)
            {
                await PublishAsync(client, _options.Warmup, measured: false);
                await probe.WaitForWarmupAsync(TimeSpan.FromSeconds(_options.RunTimeoutSeconds));
            }

            var startedAt = Stopwatch.GetTimestamp();
            var enqueueWatch = Stopwatch.StartNew();
            await PublishAsync(client, _options.JobCount, measured: true);
            enqueueWatch.Stop();

            await probe.WaitForMeasuredAsync(TimeSpan.FromSeconds(_options.RunTimeoutSeconds));
            var duration = Stopwatch.GetElapsedTime(startedAt);
            var latency = Percentiles.Compute(probe.GetLatencyMilliseconds());

            return new BrokerNativeBenchmarkResult(
                _options.JobCount,
                probe.MeasuredCompleted,
                probe.DuplicateExecutions,
                enqueueWatch.Elapsed.TotalSeconds <= 0
                    ? 0
                    : _options.JobCount / enqueueWatch.Elapsed.TotalSeconds,
                duration.TotalSeconds <= 0
                    ? 0
                    : _options.JobCount / duration.TotalSeconds,
                latency,
                enqueueWatch.Elapsed,
                duration);
        }
        finally
        {
            await host.StopAsync();
            CleanupTopology(transport);
        }
    }

    private async Task PublishAsync(IJobClient client, int count, bool measured)
    {
        if (count <= 0)
        {
            return;
        }

        var next = -1;
        var publisherCount = Math.Max(1, Math.Min(_options.SubmitterConcurrency, count));
        var publishers = Enumerable.Range(0, publisherCount)
            .Select(async _ =>
            {
                while (true)
                {
                    var index = Interlocked.Increment(ref next);
                    if (index >= count)
                    {
                        return;
                    }

                    await client.EnqueueAsync(
                        JobKey,
                        new BrokerNativeBenchPayload(
                            index,
                            Stopwatch.GetTimestamp(),
                            measured));
                }
            });

        await Task.WhenAll(publishers);
    }

    private static async Task WaitForConsumerAsync(
        string connectionString,
        string queue,
        TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow.Add(timeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            try
            {
                using var connection = CreateConnection(connectionString);
                using var channel = connection.CreateModel();
                if (channel.ConsumerCount(queue) > 0)
                {
                    return;
                }
            }
            catch (OperationInterruptedException exception)
                when (exception.ShutdownReason?.ReplyCode == 404)
            {
                // Consumer topology has not been declared yet.
            }

            await Task.Delay(50);
        }

        throw new TimeoutException($"RabbitMQ BrokerNative consumer for '{queue}' did not become ready.");
    }

    private static IConnection CreateConnection(string connectionString)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(connectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = false
        };
        return factory.CreateConnection("KubeJob.BrokerNative.Benchmark");
    }

    private static void CleanupTopology(RabbitMqBrokerNativeOptions options)
    {
        try
        {
            using var connection = CreateConnection(options.ConnectionString);
            using var channel = connection.CreateModel();
            TryDeleteQueue(channel, options.GetQueueName(LogicalQueue));
            TryDeleteQueue(channel, options.GetRetryQueueName());
            TryDeleteQueue(channel, options.GetDeadLetterQueueName());
            TryDeleteExchange(channel, options.GetRetryExchangeName());
            TryDeleteExchange(channel, options.GetDeadLetterExchangeName());
            TryDeleteExchange(channel, options.ExchangeName);
        }
        catch
        {
            // Benchmark cleanup is best effort; unique names prevent a failed
            // cleanup from corrupting a later measurement.
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
}

public sealed record BrokerNativeBenchmarkResult(
    int JobCount,
    int Completed,
    int DuplicateExecutions,
    double EnqueueTps,
    double E2eTps,
    LatencyStats Latency,
    TimeSpan EnqueueDuration,
    TimeSpan Duration);

public static class BrokerNativeResultTable
{
    public static void PrintHeader(BenchmarkOptions options)
    {
        Console.WriteLine();
        Console.WriteLine("KubeJob BrokerNative throughput benchmark");
        Console.WriteLine($"  jobs={options.JobCount} warmup={options.Warmup} work-ms={options.JobWorkMs}");
        Console.WriteLine($"  submitters={options.SubmitterConcurrency} worker-concurrency={options.WorkerMaxConcurrency}");
        Console.WriteLine("  authority=RabbitMQ delivery=BrokerNative postgres-hot-path=none");
        Console.WriteLine();
    }

    public static void Print(BrokerNativeBenchmarkResult result)
    {
        Console.WriteLine($"  jobs={result.JobCount} completed={result.Completed} duplicates={result.DuplicateExecutions}");
        Console.WriteLine("  TPS: enqueue={0,8:F1} e2e={1,8:F1}", result.EnqueueTps, result.E2eTps);
        Console.WriteLine(
            "  Latency (ms): P50={0:F2} P95={1:F2} P99={2:F2} max={3:F2} (n={4})",
            result.Latency.P50Ms,
            result.Latency.P95Ms,
            result.Latency.P99Ms,
            result.Latency.MaxMs,
            result.Latency.Samples);
        Console.WriteLine($"  enqueue-duration={result.EnqueueDuration.TotalSeconds:F2}s duration={result.Duration.TotalSeconds:F2}s");
        Console.WriteLine();
    }

    public static string ToMarkdown(BenchmarkOptions options, BrokerNativeBenchmarkResult result) =>
        $"""
        # KubeJob BrokerNative throughput benchmark

        - authority: `RabbitMQ` | delivery: `BrokerNative` | PostgreSQL hot path: `none`
        - jobs: {options.JobCount} | warmup: {options.Warmup} | work-ms: {options.JobWorkMs}
        - submitters: {options.SubmitterConcurrency} | worker-concurrency: {options.WorkerMaxConcurrency}
        - completed: {result.Completed} | duplicate executions observed: {result.DuplicateExecutions}
        - enqueue TPS: {result.EnqueueTps:F1} | E2E TPS: {result.E2eTps:F1}
        - latency ms: P50 {result.Latency.P50Ms:F2} | P95 {result.Latency.P95Ms:F2} | P99 {result.Latency.P99Ms:F2} | max {result.Latency.MaxMs:F2}
        - enqueue duration: {result.EnqueueDuration.TotalSeconds:F2}s | total duration: {result.Duration.TotalSeconds:F2}s
        """;
}

public sealed record BrokerNativeBenchPayload(
    int Index,
    long EnqueuedTimestamp,
    bool Measured);

public sealed class BrokerNativeBenchJobOptions
{
    public int WorkMs { get; init; }
}

public sealed class BrokerNativeBenchJob : IKubeJob<BrokerNativeBenchPayload>
{
    private readonly BrokerNativeBenchJobOptions _options;
    private readonly BrokerNativeBenchProbe _probe;

    public BrokerNativeBenchJob(
        BrokerNativeBenchJobOptions options,
        BrokerNativeBenchProbe probe)
    {
        _options = options;
        _probe = probe;
    }

    public async ValueTask ExecuteAsync(
        BrokerNativeBenchPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        if (_options.WorkMs > 0)
        {
            await Task.Delay(_options.WorkMs, cancellationToken);
        }

        _probe.Record(payload);
    }
}

public sealed class BrokerNativeBenchProbe
{
    private readonly int[] _warmupSeen;
    private readonly int[] _measuredSeen;
    private readonly long[] _latencyTicks;
    private readonly TaskCompletionSource _warmupDone =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _measuredDone =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _warmupCompleted;
    private int _measuredCompleted;
    private int _duplicateExecutions;

    public BrokerNativeBenchProbe(int warmupCount, int measuredCount)
    {
        _warmupSeen = new int[Math.Max(0, warmupCount)];
        _measuredSeen = new int[Math.Max(0, measuredCount)];
        _latencyTicks = new long[Math.Max(0, measuredCount)];
        if (warmupCount <= 0)
        {
            _warmupDone.TrySetResult();
        }
        if (measuredCount <= 0)
        {
            _measuredDone.TrySetResult();
        }
    }

    public int MeasuredCompleted => Volatile.Read(ref _measuredCompleted);
    public int DuplicateExecutions => Volatile.Read(ref _duplicateExecutions);

    public void Record(BrokerNativeBenchPayload payload)
    {
        if (payload.Measured)
        {
            if ((uint)payload.Index >= (uint)_measuredSeen.Length)
            {
                return;
            }

            if (Interlocked.Exchange(ref _measuredSeen[payload.Index], 1) != 0)
            {
                Interlocked.Increment(ref _duplicateExecutions);
                return;
            }

            _latencyTicks[payload.Index] = Stopwatch.GetElapsedTime(payload.EnqueuedTimestamp).Ticks;
            if (Interlocked.Increment(ref _measuredCompleted) == _measuredSeen.Length)
            {
                _measuredDone.TrySetResult();
            }
            return;
        }

        if ((uint)payload.Index >= (uint)_warmupSeen.Length)
        {
            return;
        }

        if (Interlocked.Exchange(ref _warmupSeen[payload.Index], 1) != 0)
        {
            Interlocked.Increment(ref _duplicateExecutions);
            return;
        }

        if (Interlocked.Increment(ref _warmupCompleted) == _warmupSeen.Length)
        {
            _warmupDone.TrySetResult();
        }
    }

    public Task WaitForWarmupAsync(TimeSpan timeout) => _warmupDone.Task.WaitAsync(timeout);

    public Task WaitForMeasuredAsync(TimeSpan timeout) => _measuredDone.Task.WaitAsync(timeout);

    public double[] GetLatencyMilliseconds() =>
        _latencyTicks
            .Where(ticks => ticks > 0)
            .Select(ticks => TimeSpan.FromTicks(ticks).TotalMilliseconds)
            .ToArray();
}
