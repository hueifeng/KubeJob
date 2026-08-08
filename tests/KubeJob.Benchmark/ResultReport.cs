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
    long ProcessStartMemoryBytes,
    long AllocatedBytes,
    int Gen0Collections,
    int Gen1Collections,
    int Gen2Collections,
    int MaxProcessThreads,
    int MaxThreadPoolThreads,
    long MaxWorkingSetBytes)
{
    public static MetricSamples Empty { get; } = new(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
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
        Console.WriteLine($"  submitters={opts.SubmitterConcurrency} worker-concurrency={opts.WorkerMaxConcurrency}");
        Console.WriteLine($"  outbox-concurrency={opts.OutboxPublishConcurrency} outbox-batch={opts.OutboxBatchSize}");
        Console.WriteLine($"  hotkey-count={opts.HotKeyCardinality} uniform-keys={(opts.UniformKeyCardinality == 0 ? "distinct" : opts.UniformKeyCardinality)}");
        Console.WriteLine($"  synchronous-commit={(opts.SynchronousCommitEnabled ? "on" : "off (throughput experiment; not production durability)")}");
        Console.WriteLine($"  poll-ms={opts.PollIntervalMs} status-parallelism={opts.StatusPollParallelism} "
            + $"metrics-ms={opts.MetricsIntervalMs} cpu={(opts.CpuSamplingEnabled ? "on" : "off")} "
            + "delivery=PostgresManaged");
        Console.WriteLine();
    }

    public static void PrintRow(ScenarioResult r)
    {
        Console.WriteLine($"[{r.Scenario.Label()}] ({r.Mode})");
        Console.WriteLine($"  jobs={r.JobCount} succeeded={r.Succeeded} failed={r.Failed} canceled/dead={r.CanceledOrDead}");
        Console.WriteLine("  TPS:  ingest={0,8:F1}  e2e(server)={1,8:F1}  e2e(wall)={2,8:F1}",
            r.IngestTps, r.E2eTps, r.WallClockE2eTps);
        Console.WriteLine("  Latency (ms): P50={0:F2}  P95={1:F2}  P99={2:F2}  max={3:F2}  (n={4})",
            r.Latency.P50Ms, r.Latency.P95Ms, r.Latency.P99Ms, r.Latency.MaxMs, r.Latency.Samples);
        Console.WriteLine("  Metrics:     db-conn-max={0}  rabbit-ready-max={1}  rabbit-unacked-max={2}  cpu-avg={3:F1}%  heap-max={4:F1}MB  heap-avg={5:F1}MB  rss-max={6:F1}MB  (samples={7})",
            r.Metrics.MaxDbConnections, r.Metrics.MaxReady, r.Metrics.MaxUnacked, r.Metrics.AvgCpuPct,
            r.Metrics.MaxProcessMemoryBytes / (1024.0 * 1024.0),
            r.Metrics.AvgProcessMemoryBytes / (1024.0 * 1024.0),
            r.Metrics.MaxWorkingSetBytes / (1024.0 * 1024.0),
            r.Metrics.SampleCount);
        Console.WriteLine("  Allocated:   {0:F1}MB total ({1:F0}KB/job)  Gen0={2} Gen1={3} Gen2={4}  threads(proc)={5} threads(pool)={6}",
            r.Metrics.AllocatedBytes / (1024.0 * 1024.0),
            r.JobCount == 0 ? 0 : r.Metrics.AllocatedBytes / (1024.0 * r.JobCount),
            r.Metrics.Gen0Collections, r.Metrics.Gen1Collections, r.Metrics.Gen2Collections,
            r.Metrics.MaxProcessThreads, r.Metrics.MaxThreadPoolThreads);
        Console.WriteLine($"  duration={r.Duration.TotalSeconds:F1}s");
        Console.WriteLine();
    }

    public static string ToMarkdown(BenchmarkOptions opts, IReadOnlyList<ScenarioResult> results)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# KubeJob throughput benchmark");
        sb.AppendLine();
        sb.AppendLine($"- mode: `{opts.SubmissionMode}` | jobs: {opts.JobCount} | warmup: {opts.Warmup} | work-ms: {opts.JobWorkMs}");
        sb.AppendLine($"- submitters: {opts.SubmitterConcurrency} | worker-concurrency: {opts.WorkerMaxConcurrency}");
        sb.AppendLine($"- outbox-concurrency: {opts.OutboxPublishConcurrency} | outbox-batch: {opts.OutboxBatchSize}");
        sb.AppendLine($"- hotkey-count: {opts.HotKeyCardinality} | uniform-keys: {(opts.UniformKeyCardinality == 0 ? "distinct" : opts.UniformKeyCardinality.ToString(CultureInfo.InvariantCulture))}");
        sb.AppendLine($"- synchronous-commit: {(opts.SynchronousCommitEnabled ? "on" : "off (throughput experiment; not production durability)")}");
        sb.AppendLine($"- poll-ms: {opts.PollIntervalMs} | status-parallelism: {opts.StatusPollParallelism} | metrics-ms: {opts.MetricsIntervalMs} | cpu: {(opts.CpuSamplingEnabled ? "on" : "off")} | delivery: PostgresManaged");
        sb.AppendLine();
        sb.AppendLine("| Scenario | Mode | Jobs | Succeeded | Ingest TPS | E2E TPS (server) | E2E TPS (wall) | P50 ms | P95 ms | P99 ms | Max ms | DB conn max | Rabbit ready max | Rabbit unacked max | CPU avg % | Heap max MB | RSS max MB | Alloc MB | Alloc KB/job | Gen0 | Gen1 | Gen2 | Thr(proc) | Thr(pool) | Duration s |");
        sb.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var r in results)
        {
            sb.Append("| ").Append(r.Scenario.Label())
              .Append(" | `").Append(r.Mode).Append("`")
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
              .Append(" | ").Append((r.Metrics.MaxWorkingSetBytes / (1024.0 * 1024.0)).ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append((r.Metrics.AllocatedBytes / (1024.0 * 1024.0)).ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append((r.JobCount == 0 ? 0 : r.Metrics.AllocatedBytes / (1024.0 * r.JobCount)).ToString("F0", CultureInfo.InvariantCulture))
              .Append(" | ").Append(r.Metrics.Gen0Collections)
              .Append(" | ").Append(r.Metrics.Gen1Collections)
              .Append(" | ").Append(r.Metrics.Gen2Collections)
              .Append(" | ").Append(r.Metrics.MaxProcessThreads)
              .Append(" | ").Append(r.Metrics.MaxThreadPoolThreads)
              .Append(" | ").Append(r.Duration.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture))
              .AppendLine(" |");
        }
        return sb.ToString();
    }
}
