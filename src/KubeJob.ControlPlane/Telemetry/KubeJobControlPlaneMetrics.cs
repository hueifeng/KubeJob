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

    // Observable gauges are kept alive by these fields so the meter does not
    // collect them between scrapes; the callbacks read the cached snapshot.
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
            description: "Number of newly accepted KubeJob job submissions.");
        _idempotencyHits = _meter.CreateCounter<long>(
            "kubejob.job.idempotency_hits",
            unit: "{job}",
            description: "Number of accepted KubeJob submissions resolved to an existing idempotency key.");
        _reclaimedLeases = _meter.CreateCounter<long>(
            "kubejob.control_plane.lease_reaper.reclaimed",
            unit: "{attempt}",
            description: "Number of expired job attempt leases reclaimed by the lease reaper sweep.");
        _outboxPublishLag = _meter.CreateHistogram<double>(
            "kubejob.control_plane.outbox.publish_lag",
            unit: "s",
            description: "Elapsed time between an outbox message becoming available for delivery and the moment it is published to its transport.");
        _orderingWaitDuration = _meter.CreateHistogram<double>(
            "kubejob.control_plane.ordering.wait_duration",
            unit: "s",
            description: "Wall-clock time a KeyOrdered Run waited before claim (claim time minus AvailableAt), including time blocked behind a non-terminal same-key predecessor. Parallel runs are not recorded.");
        _orderingBlockedRuns = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.blocked_runs",
            observeValues: () => ObserveInt(sample => sample.BlockedRuns),
            unit: "{run}",
            description: "KeyOrdered Pending Runs currently blocked behind a non-terminal same-key predecessor, per queue. Snapshot refreshed periodically; never queried on scrape.");
        _orderingOldestBlockedAge = _meter.CreateObservableGauge<double>(
            "kubejob.control_plane.ordering.oldest_blocked_age",
            observeValues: () => ObserveDouble(sample => sample.OldestBlockedAgeSeconds),
            unit: "s",
            description: "Age of the oldest blocked KeyOrdered Run per queue. Snapshot refreshed periodically; never queried on scrape.");
        _orderingActiveKeys = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.active_keys",
            observeValues: () => ObserveInt(sample => sample.ActiveKeys),
            unit: "{key}",
            description: "Distinct ConcurrencyKeys with at least one non-terminal KeyOrdered Run per queue. Snapshot refreshed periodically; never queried on scrape.");
        _orderingStrictFifoBlocked = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.strictfifo_blocked_runs",
            observeValues: () => ObserveInt(sample => sample.StrictFifoBlocked),
            unit: "{run}",
            description: "StrictFifo Pending Runs blocked because a prior Run on the same queue is still inflight. Snapshot refreshed periodically.");
        _orderingRetryBlocked = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.retry_blocked_runs",
            observeValues: () => ObserveInt(sample => sample.RetryBlockedRuns),
            unit: "{run}",
            description: "Blocked Runs whose predecessor is retrying (attempt > 1). Helps detect retry-storm stalls.");
        _orderingLaneBacklog = _meter.CreateObservableGauge<int>(
            "kubejob.control_plane.ordering.lane_blocked_runs",
            observeValues: () => ObserveLaneBacklog(),
            unit: "{run}",
            description: "Per-lane blocked run count within a queue. Tagged with kubejob.lane_id.");
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

    public bool IsOrderingWaitEnabled => _orderingWaitDuration.Enabled;

    public void OrderingClaimed(TimeSpan wait, string queue)
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

    /// <summary>
    /// Replaces the cached ordering backlog snapshot that backs the observable
    /// gauges. Called periodically by <c>OrderingMetricsRefreshService</c>; a
    /// metrics scrape reads this cache and never triggers a store query.
    /// </summary>
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
