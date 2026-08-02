using KubeJob.Core.Attributes;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Benchmark;

/// <summary>
/// Minimal payload for the benchmark job. Fields are intentionally trivial so
/// serialization and deserialization cost stay constant across scenarios; the
/// measured load is the durable pipeline (outbox, broker dispatch, admission,
/// completion), not payload handling.
/// </summary>
public sealed record BenchPayload(int Value = 0);

/// <summary>
/// Per-job execution behavior. <see cref="WorkMs"/> simulates CPU-bound handler
/// work; the default of zero measures the pipeline ceiling (overhead-bound),
/// while a positive value exposes contention in the KeyOrdered hot-key case.
/// </summary>
public sealed class BenchJobOptions
{
    public int WorkMs { get; set; }
}

/// <summary>
/// No-op handler registered under a stable job key. It never fails and never
/// retries, so every submitted Run reaches a terminal Succeeded phase and the
/// benchmark measures pure pipeline throughput and latency.
/// </summary>
[KubeJob(BenchJobKeyString)]
public sealed class NoopBenchJob : IKubeJob<BenchPayload>
{
    public const string BenchJobKeyString = "bench.noop";

    /// <summary>
    /// Stable <see cref="KubeJob.Core.Jobs.JobKey{T}"/> shared by the typed
    /// submission path and the RabbitMQ ingress envelope, so both entry
    /// points dispatch to this same handler.
    /// </summary>
    public static KubeJob.Core.Jobs.JobKey<BenchPayload> JobKey { get; } =
        new(BenchJobKeyString);

    private readonly BenchJobOptions _options;

    public NoopBenchJob(BenchJobOptions options) => _options = options;

    public async ValueTask ExecuteAsync(
        BenchPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        if (_options.WorkMs > 0)
        {
            await Task.Delay(_options.WorkMs, cancellationToken);
        }
    }
}