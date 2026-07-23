using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace KubeJob.Server.Telemetry;

/// <summary>Stable instrumentation surface for runtime operators and OpenTelemetry exporters.</summary>
public static class KubeJobTelemetry
{
    public const string ActivitySourceName = "KubeJob.Runtime";
    public const string MeterName = "KubeJob.Runtime";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    public static readonly Meter Meter = new(MeterName);
    public static readonly Counter<long> Claims = Meter.CreateCounter<long>("kubejob.claims");
    public static readonly Counter<long> Completions = Meter.CreateCounter<long>("kubejob.completions");
    public static readonly Counter<long> FencedRejects = Meter.CreateCounter<long>("kubejob.fenced_rejects");
    public static readonly Histogram<double> ClaimLatency = Meter.CreateHistogram<double>("kubejob.claim_latency_ms");
}
