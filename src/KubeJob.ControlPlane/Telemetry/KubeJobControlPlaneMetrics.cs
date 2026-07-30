using System.Diagnostics.Metrics;
using KubeJob.Core.Telemetry;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.ControlPlane.Telemetry;

public sealed class KubeJobControlPlaneMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _submissions;
    private readonly Counter<long> _idempotencyHits;

    public KubeJobControlPlaneMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(
            KubeJobTelemetry.ControlPlaneMeterName,
            typeof(KubeJobControlPlaneMetrics).Assembly.GetName().Version?.ToString());

        _submissions = _meter.CreateCounter<long>(
            "kubejob.job.submissions",
            unit: "{job}",
            description: "Number of newly accepted KubeJob job submissions.");
        _idempotencyHits = _meter.CreateCounter<long>(
            "kubejob.job.idempotency_hits",
            unit: "{job}",
            description: "Number of accepted KubeJob submissions resolved to an existing idempotency key.");
    }

    public void SubmissionCompleted(bool existing)
    {
        var instrument = existing ? _idempotencyHits : _submissions;
        if (!instrument.Enabled)
        {
            return;
        }

        instrument.Add(1);
    }

    public void Dispose() => _meter.Dispose();
}
