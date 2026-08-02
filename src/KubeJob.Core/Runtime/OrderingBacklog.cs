namespace KubeJob.Core.Runtime;

/// <summary>
/// Per-queue snapshot of the ordering backlog. The runtime dashboard
/// store produces this on a fixed refresh cadence; the control-plane meter caches
/// it and exposes it through observable gauges, so a metrics scrape never
/// triggers a database query or a table scan. A queue with no ordering
/// activity emits no sample.
/// </summary>
public sealed record OrderingBacklogSample(
    string Queue,
    int BlockedRuns,
    double OldestBlockedAgeSeconds,
    int ActiveKeys,
    /// <summary>
    /// Number of StrictFifo-blocked runs on this queue. These are counted
    /// separately because StrictFifo blocks the entire lane, not just a key.
    /// </summary>
    int StrictFifoBlocked,
    /// <summary>
    /// Number of blocked runs that are behind a retrying predecessor
    /// (i.e. predecessor is on attempt > 1). This helps diagnose
    /// retry-storm-induced ordering stalls.
    /// </summary>
    int RetryBlockedRuns,
    /// <summary>
    /// Per-lane breakdown for this queue. Empty when lanes are not used.
    /// The first element corresponds to lane-0, etc.
    /// </summary>
    IReadOnlyList<LaneBacklogSample> LaneBreakdown);

/// <summary>
/// Per-lane snapshot of ordering activity within a single queue.
/// </summary>
public sealed record LaneBacklogSample(
    string Queue,
    int LaneId,
    int BlockedRuns,
    double OldestBlockedAgeSeconds,
    int ActiveKeys,
    long OldestBlockedOrderingSequence);