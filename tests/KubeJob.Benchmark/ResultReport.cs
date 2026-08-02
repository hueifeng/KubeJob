using System.Globalization;
using System.Text;

namespace KubeJob.Benchmark;

public sealed record LatencyStats(double P50Ms, double P95Ms, double P99Ms, double MaxMs, int Samples)
{
    public static LatencyStats Empty { get; } = new(0, 0, 0, 0, 0);
}

public sealed record MetricSamples(
    int MaxDbConnections,
    int MaxReady,
    int MaxUnacked,
    double AvgCpuPct,
    int SampleCount,
    long MaxProcessMemoryBytes,
    double AvgProcessMemoryBytes,
    long ProcessStartMemoryBytes)
{
    public static MetricSamples Empty { get; } = new(0, 0, 0, 0, 0, 0, 0, 0);
}

public sealed record ScenarioResult(
    BenchScenario Scenario,
    int JobCount,
    int Succeeded,
    int Failed,
    int CanceledOrDead,
    double IngestTps,
    double E2eTps,
    double WallClockE2eTps,
    LatencyStats Latency,
    MetricSamples Metrics,
    TimeSpan Duration)
{
    public string Mode { get; init; } = string.Empty;
    /// <summary>Execution lane count (1 = shared queue, N > 1 = per-lane queues).</summary>
    public int LaneCount { get; init; } = 1;
}

/// <summary>
/// Nearest-rank percentile computation and a console/markdown results table.
/// No external statistics dependency is required.
/// </summary>
public static class Percentiles
{
    public static LatencyStats Compute(double[] samplesMs)
    {
        if (samplesMs.Length == 0) return LatencyStats.Empty;
        Array.Sort(samplesMs);
        return new LatencyStats(
            Rank(samplesMs, 0.50),
            Rank(samplesMs, 0.95),
            Rank(samplesMs, 0.99),
            samplesMs[^1],
            samplesMs.Length);
    }

    private static double Rank(double[] sorted, double p)
    {
        // Nearest-rank: the ceil(p*N)-th element (1-indexed), clamped to bounds.
        var rank = (int)Math.Ceiling(p * sorted.Length);
        if (rank < 1) rank = 1;
        if (rank > sorted.Length) rank = sorted.Length;
        return sorted[rank - 1];
    }
}

public static class ResultTable
{
    public static void PrintHeader(BenchmarkOptions opts)
    {
        Console.WriteLine();
        Console.WriteLine("KubeJob throughput benchmark");
        Console.WriteLine($"  mode={opts.SubmissionMode} jobs={opts.JobCount} warmup={opts.Warmup} work-ms={opts.JobWorkMs}");
        Console.WriteLine($"  submitters={opts.SubmitterConcurrency} worker-concurrency={opts.WorkerMaxConcurrency} "
            + $"prefetch={opts.PrefetchCount} dispatch-concurrency={opts.ConsumerDispatchConcurrency}");
        Console.WriteLine($"  outbox-concurrency={opts.OutboxPublishConcurrency} outbox-batch={opts.OutboxBatchSize} "
            + $"publisher-concurrency={opts.PublisherConcurrency}");
        Console.WriteLine($"  hotkey-count={opts.HotKeyCardinality} uniform-keys={(opts.UniformKeyCardinality == 0 ? "distinct" : opts.UniformKeyCardinality)}");
        Console.WriteLine($"  lane-sweep=[{string.Join(",", opts.LaneCountSweep)}]");
        Console.WriteLine($"  poll-ms={opts.PollIntervalMs} status-parallelism={opts.StatusPollParallelism} "
            + $"metrics-ms={opts.MetricsIntervalMs} cpu={(opts.CpuSamplingEnabled ? "on" : "off")} "
            + $"delivery={opts.DeliveryProfile}");
        Console.WriteLine();
    }

    public static void PrintRow(ScenarioResult r)
    {
        var laneTag = r.LaneCount > 1 ? $" lanes={r.LaneCount}" : "";
        Console.WriteLine($"[{r.Scenario.Label()}] ({r.Mode}{laneTag})");
        Console.WriteLine($"  jobs={r.JobCount} succeeded={r.Succeeded} failed={r.Failed} canceled/dead={r.CanceledOrDead}");
        Console.WriteLine("  TPS:  ingest={0,8:F1}  e2e(server)={1,8:F1}  e2e(wall)={2,8:F1}",
            r.IngestTps, r.E2eTps, r.WallClockE2eTps);
        Console.WriteLine("  Latency (ms): P50={0:F2}  P95={1:F2}  P99={2:F2}  max={3:F2}  (n={4})",
            r.Latency.P50Ms, r.Latency.P95Ms, r.Latency.P99Ms, r.Latency.MaxMs, r.Latency.Samples);
        Console.WriteLine("  Metrics:     db-conn-max={0}  rabbit-ready-max={1}  rabbit-unacked-max={2}  cpu-avg={3:F1}%  mem-max={4:F1}MB  mem-avg={5:F1}MB  (samples={6})",
            r.Metrics.MaxDbConnections, r.Metrics.MaxReady, r.Metrics.MaxUnacked, r.Metrics.AvgCpuPct,
            r.Metrics.MaxProcessMemoryBytes / (1024.0 * 1024.0),
            r.Metrics.AvgProcessMemoryBytes / (1024.0 * 1024.0),
            r.Metrics.SampleCount);
        Console.WriteLine($"  duration={r.Duration.TotalSeconds:F1}s");
        Console.WriteLine();
    }

    public static string ToMarkdown(BenchmarkOptions opts, IReadOnlyList<ScenarioResult> results)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# KubeJob throughput benchmark");
        sb.AppendLine();
        sb.AppendLine($"- mode: `{opts.SubmissionMode}` | jobs: {opts.JobCount} | warmup: {opts.Warmup} | work-ms: {opts.JobWorkMs}");
        sb.AppendLine($"- submitters: {opts.SubmitterConcurrency} | worker-concurrency: {opts.WorkerMaxConcurrency} | prefetch: {opts.PrefetchCount} | dispatch-concurrency: {opts.ConsumerDispatchConcurrency}");
        sb.AppendLine($"- outbox-concurrency: {opts.OutboxPublishConcurrency} | outbox-batch: {opts.OutboxBatchSize} | publisher-concurrency: {opts.PublisherConcurrency}");
        sb.AppendLine($"- hotkey-count: {opts.HotKeyCardinality} | uniform-keys: {(opts.UniformKeyCardinality == 0 ? "distinct" : opts.UniformKeyCardinality.ToString(CultureInfo.InvariantCulture))}");
        sb.AppendLine($"- lane-sweep: [{string.Join(",", opts.LaneCountSweep)}]");
        sb.AppendLine($"- poll-ms: {opts.PollIntervalMs} | status-parallelism: {opts.StatusPollParallelism} | metrics-ms: {opts.MetricsIntervalMs} | cpu: {(opts.CpuSamplingEnabled ? "on" : "off")} | delivery: {opts.DeliveryProfile}");
        sb.AppendLine();
        sb.AppendLine("| Scenario | Mode | Lanes | Jobs | Succeeded | Ingest TPS | E2E TPS (server) | E2E TPS (wall) | P50 ms | P95 ms | P99 ms | Max ms | DB conn max | Rabbit ready max | Rabbit unacked max | CPU avg % | Mem max MB | Duration s |");
        sb.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var r in results)
        {
            sb.Append("| ").Append(r.Scenario.Label())
              .Append(" | `").Append(r.Mode).Append("` | ").Append(r.LaneCount)
              .Append(" | ").Append(r.JobCount)
              .Append(" | ").Append(r.Succeeded)
              .Append(" | ").Append(r.IngestTps.ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append(r.E2eTps.ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append(r.WallClockE2eTps.ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append(r.Latency.P50Ms.ToString("F2", CultureInfo.InvariantCulture))
              .Append(" | ").Append(r.Latency.P95Ms.ToString("F2", CultureInfo.InvariantCulture))
              .Append(" | ").Append(r.Latency.P99Ms.ToString("F2", CultureInfo.InvariantCulture))
              .Append(" | ").Append(r.Latency.MaxMs.ToString("F2", CultureInfo.InvariantCulture))
              .Append(" | ").Append(r.Metrics.MaxDbConnections)
              .Append(" | ").Append(r.Metrics.MaxReady)
              .Append(" | ").Append(r.Metrics.MaxUnacked)
              .Append(" | ").Append(r.Metrics.AvgCpuPct.ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append((r.Metrics.MaxProcessMemoryBytes / (1024.0 * 1024.0)).ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append(r.Duration.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture))
              .AppendLine(" |");
        }
        return sb.ToString();
    }
}