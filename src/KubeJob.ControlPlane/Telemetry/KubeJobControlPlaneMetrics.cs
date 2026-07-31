using System.Diagnostics;
using System.Diagnostics.Metrics;
using KubeJob.Core.Telemetry;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.ControlPlane.Telemetry;

public sealed class KubeJobControlPlaneMetrics : IDisposable
{
    private const string AdmissionStatusTagName = "kubejob.admission.status";

    private readonly Meter _meter;
    private readonly Counter<long> _submissions;
    private readonly Counter<long> _idempotencyHits;
    private readonly Histogram<double> _admissionDuration;
    private readonly Counter<long> _reclaimedLeases;
    private readonly Histogram<double> _outboxPublishLag;

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
        _admissionDuration = _meter.CreateHistogram<double>(
            "kubejob.control_plane.admission.duration",
            unit: "s",
            description: "Duration of a single-Run BrokerDispatch admission call (WorkerControlPlane.AdmitAsync), including its underlying Claim.");
        _reclaimedLeases = _meter.CreateCounter<long>(
            "kubejob.control_plane.lease_reaper.reclaimed",
            unit: "{attempt}",
            description: "Number of expired job attempt leases reclaimed by the lease reaper sweep.");
        _outboxPublishLag = _meter.CreateHistogram<double>(
            "kubejob.control_plane.outbox.publish_lag",
            unit: "s",
            description: "Elapsed time between an outbox message becoming available for delivery and the moment it is published to its transport.");
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

    public bool IsAdmissionDurationEnabled => _admissionDuration.Enabled;

    public void AdmissionCompleted(TimeSpan duration, string status)
    {
        if (!_admissionDuration.Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            { AdmissionStatusTagName, status }
        };
        _admissionDuration.Record(duration.TotalSeconds, tags);
    }

    public void LeasesReclaimed(int count)
    {
        if (count <= 0 || !_reclaimedLeases.Enabled)
        {
            return;
        }

        _reclaimedLeases.Add(count);
    }

    public bool IsOutboxPublishLagEnabled => _outboxPublishLag.Enabled;

    public void OutboxPublished(TimeSpan lag)
    {
        if (!_outboxPublishLag.Enabled)
        {
            return;
        }

        _outboxPublishLag.Record(lag.TotalSeconds);
    }

    public void Dispose() => _meter.Dispose();
}
