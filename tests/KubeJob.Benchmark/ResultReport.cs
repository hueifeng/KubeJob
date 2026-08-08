using System.Globalization;
using System.Text;
using KubeJob.Core.Runtime;

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
    QueueRuntimeMode RuntimeMode,
    int JobCount,
    int Succeeded,
    int Failed,
    int Incomplete,
    double IngestTps,
    double E2eTps,
    LatencyStats Latency,
    MetricSamples Metrics,
    TimeSpan Duration);

public static class Percentiles
{
    public static LatencyStats Compute(double[] samplesMs)
    {
        if (samplesMs.Length == 0)
        {
            return LatencyStats.Empty;
        }

        Array.Sort(samplesMs);
        return new LatencyStats(
            Rank(samplesMs, 0.50),
            Rank(samplesMs, 0.95),
            Rank(samplesMs, 0.99),
            samplesMs[^1],
            samplesMs.Length);
    }

    private static double Rank(double[] sorted, double percentile)
    {
        var rank = Math.Clamp((int)Math.Ceiling(percentile * sorted.Length), 1, sorted.Length);
        return sorted[rank - 1];
    }
}

public static class ResultTable
{
    public static void PrintHeader(BenchmarkOptions opts)
    {
        Console.WriteLine();
        Console.WriteLine("KubeJob V3 throughput benchmark");
        Console.WriteLine($"  runtime={opts.RuntimeMode} jobs={opts.JobCount} warmup={opts.Warmup} work-ms={opts.JobWorkMs}");
        Console.WriteLine($"  submitters={opts.SubmitterConcurrency} worker-concurrency={opts.WorkerMaxConcurrency} prefetch={opts.PrefetchCount}");
        Console.WriteLine($"  managed-outbox-concurrency={opts.OutboxPublishConcurrency} managed-outbox-batch={opts.OutboxBatchSize}");
        Console.WriteLine($"  metrics-ms={opts.MetricsIntervalMs} cpu={(opts.CpuSamplingEnabled ? "on" : "off")}");
        Console.WriteLine("  BrokerNative invariant: normal publish/consume/handler/ACK path must not open PostgreSQL connections.");
        Console.WriteLine();
    }

    public static void PrintRow(ScenarioResult result)
    {
        Console.WriteLine($"[{result.Scenario.Label()}] ({result.RuntimeMode})");
        Console.WriteLine($"  jobs={result.JobCount} succeeded={result.Succeeded} failed={result.Failed} incomplete={result.Incomplete}");
        Console.WriteLine("  TPS: ingest={0,8:F1} e2e={1,8:F1}", result.IngestTps, result.E2eTps);
        Console.WriteLine("  Latency (ms): P50={0:F2} P95={1:F2} P99={2:F2} max={3:F2} (n={4})",
            result.Latency.P50Ms, result.Latency.P95Ms, result.Latency.P99Ms, result.Latency.MaxMs, result.Latency.Samples);
        Console.WriteLine("  Metrics: db-conn-max={0} rabbit-ready-max={1} rabbit-unacked-max={2} cpu-avg={3:F1}% heap-max={4:F1}MB rss-max={5:F1}MB",
            result.Metrics.MaxDbConnections,
            result.Metrics.MaxReady,
            result.Metrics.MaxUnacked,
            result.Metrics.AvgCpuPct,
            result.Metrics.MaxProcessMemoryBytes / (1024.0 * 1024.0),
            result.Metrics.MaxWorkingSetBytes / (1024.0 * 1024.0));
        Console.WriteLine("  Allocated: {0:F1}MB ({1:F0}KB/job) Gen0={2} Gen1={3} Gen2={4} threads(proc)={5} threads(pool)={6}",
            result.Metrics.AllocatedBytes / (1024.0 * 1024.0),
            result.JobCount == 0 ? 0 : result.Metrics.AllocatedBytes / (1024.0 * result.JobCount),
            result.Metrics.Gen0Collections,
            result.Metrics.Gen1Collections,
            result.Metrics.Gen2Collections,
            result.Metrics.MaxProcessThreads,
            result.Metrics.MaxThreadPoolThreads);
        Console.WriteLine($"  duration={result.Duration.TotalSeconds:F1}s");
        Console.WriteLine();
    }

    public static string ToMarkdown(BenchmarkOptions opts, IReadOnlyList<ScenarioResult> results)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# KubeJob V3 throughput benchmark");
        sb.AppendLine();
        sb.AppendLine($"- runtime: `{opts.RuntimeMode}` | jobs: {opts.JobCount} | warmup: {opts.Warmup} | work-ms: {opts.JobWorkMs}");
        sb.AppendLine($"- submitters: {opts.SubmitterConcurrency} | worker-concurrency: {opts.WorkerMaxConcurrency} | prefetch: {opts.PrefetchCount}");
        sb.AppendLine($"- normal PostgreSQL durability is enabled; the harness no longer sets `synchronous_commit=off`.");
        sb.AppendLine();
        sb.AppendLine("| Scenario | Runtime | Jobs | Succeeded | Incomplete | Ingest TPS | E2E TPS | P50 ms | P95 ms | P99 ms | Max ms | DB conn max | Rabbit ready max | Rabbit unacked max | CPU avg % | Heap max MB | RSS max MB | Duration s |");
        sb.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var result in results)
        {
            sb.Append("| ").Append(result.Scenario.Label())
              .Append(" | `").Append(result.RuntimeMode).Append('`')
              .Append(" | ").Append(result.JobCount)
              .Append(" | ").Append(result.Succeeded)
              .Append(" | ").Append(result.Incomplete)
              .Append(" | ").Append(result.IngestTps.ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append(result.E2eTps.ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append(result.Latency.P50Ms.ToString("F2", CultureInfo.InvariantCulture))
              .Append(" | ").Append(result.Latency.P95Ms.ToString("F2", CultureInfo.InvariantCulture))
              .Append(" | ").Append(result.Latency.P99Ms.ToString("F2", CultureInfo.InvariantCulture))
              .Append(" | ").Append(result.Latency.MaxMs.ToString("F2", CultureInfo.InvariantCulture))
              .Append(" | ").Append(result.Metrics.MaxDbConnections)
              .Append(" | ").Append(result.Metrics.MaxReady)
              .Append(" | ").Append(result.Metrics.MaxUnacked)
              .Append(" | ").Append(result.Metrics.AvgCpuPct.ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append((result.Metrics.MaxProcessMemoryBytes / (1024.0 * 1024.0)).ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append((result.Metrics.MaxWorkingSetBytes / (1024.0 * 1024.0)).ToString("F1", CultureInfo.InvariantCulture))
              .Append(" | ").Append(result.Duration.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture))
              .AppendLine(" |");
        }

        return sb.ToString();
    }
}
