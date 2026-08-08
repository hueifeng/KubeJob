using System.Diagnostics;
using System.Diagnostics.Metrics;
using KubeJob.Core.Runtime;
using KubeJob.Core.Telemetry;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.ControlPlane.Telemetry;

public sealed class KubeJobControlPlaneMetrics : IDisposable
{
    private const string QueueTagName = "kubejob.queue";

    private readonly Meter _meter;
    private readonly Counter<long> _submissions;
    private readonly Counter<long> _idempotencyHits;
    private readonly Counter<long> _reclaimedLeases;
    private readonly Histogram<double> _outboxPublishLag;
    private readonly Histogram<double> _orderingWaitDuration;

    private readonly ObservableGauge<int> _orderingBlockedRuns;
    private readonly ObservableGauge<double> _orderingOldestBlockedAge;
    private readonly ObservableGauge<int> _orderingActiveKeys;
    private readonly ObservableGauge<int> _orderingStrictFifoBlocked;
    private readonly ObservableGauge<int> _orderingRetryBlocked;
    private readonly ObservableGauge<int> _orderingLaneBacklog;
    private readonly object _orderingBacklogGate = new();
    private IReadOnlyList<OrderingBacklogSample> _orderingBacklog = Array.Empty<OrderingBacklogSample>();

    public KubeJobControlPlaneMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(
            KubeJobTelemetry.ControlPlaneMeterName,
            typeof(KubeJobControlPlaneMetrics).Assembly.GetName().Version?.ToString());

        _submissions = _meter.CreateCounter<long>(
            "kubejob.job.submissions",
            unit: "{job}",
            description: "Number of newly accepted PostgresManaged job submissions.");
        _idempotencyHits = _meter.CreateCounter<long>(
            "kubejob.job.idempotency_hits",
            unit: "{job}",
            description: "Number of PostgresManaged submissions resolved to an existing idempotency key.");
        _reclaimedLeases = _meter.CreateCounter<long>(
            "kubejob.control_plane.lease_reaper.reclaimed",
            unit: "{attempt}",
            description: "Number of expired PostgresManaged attempt leases reclaimed by the lease reaper sweep.");
        _outboxPublishLag = _meter.CreateHistogram<double>(
            "kubejob.control_plane.outbox.publish_lag",
            unit: "s",
            description: "Elapsed time between a managed wake outbox row becoming available and publication of its optional wake signal.");
        _orderingWaitDuration = _meter.CreateHistogram<double>(
            "kubejob.control_plane.ordering.wait_duration",
            unit: "s",
            description: "Wall-clock time a PostgresManaged KeyOrdered Run waited before claim. Parallel runs are not recorded.");
        _orderingBlockedRuns = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.blocked_runs",
            observeValues: () => ObserveInt(sample => sample.BlockedRuns),
            unit: "{run}",
            description: "PostgresManaged KeyOrdered Pending Runs blocked behind a non-terminal same-key predecessor, per queue.");
        _orderingOldestBlockedAge = _meter.CreateObservableGauge<double>(
            "kubejob.control_plane.ordering.oldest_blocked_age",
            observeValues: () => ObserveDouble(sample => sample.OldestBlockedAgeSeconds),
            unit: "s",
            description: "Age of the oldest blocked PostgresManaged KeyOrdered Run per queue.");
        _orderingActiveKeys = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.active_keys",
            observeValues: () => ObserveInt(sample => sample.ActiveKeys),
            unit: "{key}",
            description: "Distinct keys with at least one non-terminal PostgresManaged KeyOrdered Run per queue.");
        _orderingStrictFifoBlocked = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.strictfifo_blocked_runs",
            observeValues: () => ObserveInt(sample => sample.StrictFifoBlocked),
            unit: "{run}",
            description: "PostgresManaged StrictFifo Pending Runs blocked behind prior inflight Runs on the same queue.");
        _orderingRetryBlocked = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.retry_blocked_runs",
            observeValues: () => ObserveInt(sample => sample.RetryBlockedRuns),
            unit: "{run}",
            description: "Blocked managed Runs whose predecessor is retrying.");
        _orderingLaneBacklog = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.lane_blocked_runs",
            observeValues: () => ObserveLaneBacklog(),
            unit: "{run}",
            description: "Managed execution-lane blocked Run count per queue. Tagged with kubejob.lane_id.");
    }

    public void SubmissionCompleted(bool existing)
    {
        var instrument = existing ? _idempotencyHits : _submissions;
        if (instrument.Enabled)
        {
            instrument.Add(1);
        }
    }

    public void LeasesReclaimed(int count)
    {
        if (count > 0 && _reclaimedLeases.Enabled)
        {
            _reclaimedLeases.Add(count);
        }
    }

    public bool IsOutboxPublishLagEnabled => _outboxPublishLag.Enabled;

    public void OutboxPublished(TimeSpan lag)
    {
        if (_outboxPublishLag.Enabled)
        {
            _outboxPublishLag.Record(lag.TotalSeconds);
        }
    }

    public bool IsOrderingWaitEnabled => _orderingWaitDuration.Enabled;

    public void OrderingAdmitted(TimeSpan wait, string queue)
    {
        if (!_orderingWaitDuration.Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            { QueueTagName, queue }
        };
        _orderingWaitDuration.Record(wait.TotalSeconds, tags);
    }

    public void UpdateOrderingBacklog(IReadOnlyList<OrderingBacklogSample> samples)
    {
        ArgumentNullException.ThrowIfNull(samples);
        lock (_orderingBacklogGate)
        {
            _orderingBacklog = samples;
        }
    }

    private IEnumerable<Measurement<int>> ObserveInt(Func<OrderingBacklogSample, int> selector)
    {
        foreach (var sample in CurrentBacklog())
        {
            var tags = new TagList { { QueueTagName, sample.Queue } };
            yield return new Measurement<int>(selector(sample), tags);
        }
    }

    private IEnumerable<Measurement<double>> ObserveDouble(Func<OrderingBacklogSample, double> selector)
    {
        foreach (var sample in CurrentBacklog())
        {
            var tags = new TagList { { QueueTagName, sample.Queue } };
            yield return new Measurement<double>(selector(sample), tags);
        }
    }

    private IEnumerable<Measurement<int>> ObserveLaneBacklog()
    {
        foreach (var sample in CurrentBacklog())
        {
            foreach (var lane in sample.LaneBreakdown)
            {
                var tags = new TagList
                {
                    { QueueTagName, sample.Queue },
                    { "kubejob.lane_id", lane.LaneId.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                };
                yield return new Measurement<int>(lane.BlockedRuns, tags);
            }
        }
    }

    private IReadOnlyList<OrderingBacklogSample> CurrentBacklog()
    {
        lock (_orderingBacklogGate)
        {
            return _orderingBacklog;
        }
    }

    public void Dispose() => _meter.Dispose();
}
