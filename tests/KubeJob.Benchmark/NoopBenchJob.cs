using System.Collections.Concurrent;
using KubeJob.Core.Attributes;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;

namespace KubeJob.Benchmark;

public sealed record BenchPayload(int Value = 0, long EnqueuedUtcTicks = 0);

public sealed class BenchJobOptions
{
    public int WorkMs { get; set; }
}

public sealed record BenchCompletionSnapshot(int Completed, double[] LatencySamplesMs);

/// <summary>
/// Process-local completion tracker shared by both runtime modes. BrokerNative
/// benchmarks therefore never query JobRun state merely to discover that a job
/// completed, keeping PostgreSQL out of the measured broker hot path.
/// </summary>
public sealed class BenchCompletionTracker
{
    private readonly object _gate = new();
    private ConcurrentQueue<double> _latencies = new();
    private TaskCompletionSource<bool> _completed = NewCompletionSource();
    private int _expected;
    private int _count;

    public void Begin(int expected)
    {
        lock (_gate)
        {
            _expected = Math.Max(0, expected);
            _count = 0;
            _latencies = new ConcurrentQueue<double>();
            _completed = NewCompletionSource();
            if (_expected == 0)
            {
                _completed.TrySetResult(true);
            }
        }
    }

    public void Record(long enqueuedUtcTicks)
    {
        if (enqueuedUtcTicks > 0)
        {
            var elapsedTicks = Math.Max(0, DateTimeOffset.UtcNow.UtcTicks - enqueuedUtcTicks);
            _latencies.Enqueue(TimeSpan.FromTicks(elapsedTicks).TotalMilliseconds);
        }

        var count = Interlocked.Increment(ref _count);
        if (count >= Volatile.Read(ref _expected))
        {
            _completed.TrySetResult(true);
        }
    }

    public async Task<BenchCompletionSnapshot> WaitAsync(TimeSpan timeout)
    {
        try
        {
            await _completed.Task.WaitAsync(timeout);
        }
        catch (TimeoutException)
        {
            // Return the partial snapshot; the caller reports the remainder as
            // incomplete instead of hiding a throughput failure.
        }

        return new BenchCompletionSnapshot(
            Math.Min(Volatile.Read(ref _count), Volatile.Read(ref _expected)),
            _latencies.ToArray());
    }

    private static TaskCompletionSource<bool> NewCompletionSource() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}

[KubeJob(BenchJobKeyString)]
public sealed class NoopBenchJob : IKubeJob<BenchPayload>
{
    public const string BenchJobKeyString = "bench.noop";

    public static KubeJob.Core.Jobs.JobKey<BenchPayload> JobKey { get; } =
        new(BenchJobKeyString);

    private readonly BenchJobOptions _options;
    private readonly BenchCompletionTracker _tracker;

    public NoopBenchJob(BenchJobOptions options, BenchCompletionTracker tracker)
    {
        _options = options;
        _tracker = tracker;
    }

    public async ValueTask ExecuteAsync(
        BenchPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        if (_options.WorkMs > 0)
        {
            await Task.Delay(_options.WorkMs, cancellationToken);
        }

        _tracker.Record(payload.EnqueuedUtcTicks);
    }
}
