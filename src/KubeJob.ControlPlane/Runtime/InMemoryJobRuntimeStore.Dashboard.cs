using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.ControlPlane.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<DashboardOverview> GetOverviewAsync(
        int recentRunCount,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var observedAt = DateTimeOffset.UtcNow;
            var activityWindowStart = observedAt.AddHours(-1);
            var runs = _runs.Values.ToArray();
            var sessions = _sessions.Values.ToArray();
            var schedules = _schedules.Values.ToArray();
            var queues = runs
                .Where(run => run.Phase is JobPhase.Pending or JobPhase.Running)
                .GroupBy(run => run.Queue, StringComparer.Ordinal)
                .Select(group => new DashboardQueueSummary(
                    group.Key,
                    group.Count(run => run.Phase == JobPhase.Pending),
                    group.Count(run => run.Phase == JobPhase.Running),
                    group
                        .Where(run => run.Phase == JobPhase.Pending && run.AvailableAt <= observedAt)
                        .Select(run => (DateTimeOffset?)run.AvailableAt)
                        .Min()))
                .OrderByDescending(queue => queue.ActiveRuns)
                .ThenBy(queue => queue.Queue, StringComparer.Ordinal)
                .Take(12)
                .ToArray();
            var recent = runs
                .OrderByDescending(run => run.CreatedAt)
                .ThenByDescending(run => run.Id, StringComparer.Ordinal)
                .Take(Math.Clamp(recentRunCount, 1, 100))
                .Select(ToDashboardSummary)
                .ToArray();
            var lastHourRuns = runs
                .Where(run => run.CompletedAt is { } completedAt
                              && completedAt >= activityWindowStart
                              && completedAt <= observedAt)
                .ToArray();
            var pendingOutbox = _outbox.Values
                .Where(message => message.State is OutboxDeliveryState.Pending or OutboxDeliveryState.Publishing or OutboxDeliveryState.Failed)
                .ToArray();
            var oldestReadyRunAt = runs
                .Where(run => run.Phase == JobPhase.Pending && run.AvailableAt <= observedAt)
                .Select(run => (DateTimeOffset?)run.AvailableAt)
                .Min();

            return ValueTask.FromResult(new DashboardOverview(
                ObservedAt: observedAt,
                PendingRuns: runs.Count(run => run.Phase == JobPhase.Pending),
                RunningRuns: runs.Count(run => run.Phase == JobPhase.Running),
                SucceededRuns: runs.Count(run => run.Phase == JobPhase.Succeeded),
                FailedRuns: runs.Count(run => run.Phase == JobPhase.Failed),
                CanceledRuns: runs.Count(run => run.Phase == JobPhase.Canceled),
                DeadRuns: runs.Count(run => run.Phase == JobPhase.Dead),
                ReadyWorkers: sessions.Count(session => session.State == WorkerSessionState.Ready),
                DrainingWorkers: sessions.Count(session => session.State == WorkerSessionState.Draining),
                TotalWorkerCapacity: sessions
                    .Where(session => session.State is WorkerSessionState.Ready or WorkerSessionState.Draining)
                    .Sum(session => session.MaxConcurrency),
                AvailableWorkerSlots: sessions
                    .Where(session => session.State == WorkerSessionState.Ready)
                    .Sum(session => session.AvailableSlots),
                EnabledSchedules: schedules.Count(schedule => schedule.Enabled),
                DisabledSchedules: schedules.Count(schedule => !schedule.Enabled),
                PendingOutboxMessages: pendingOutbox.Length,
                LastHour: new DashboardActivitySummary(
                    lastHourRuns.Count(run => run.Phase == JobPhase.Succeeded),
                    lastHourRuns.Count(run => run.Phase == JobPhase.Failed),
                    lastHourRuns.Count(run => run.Phase == JobPhase.Canceled),
                    lastHourRuns.Count(run => run.Phase == JobPhase.Dead)),
                Queues: queues,
                RecentRuns: recent,
                FailedOutboxMessages: pendingOutbox.Count(message => message.State == OutboxDeliveryState.Failed),
                OldestPendingOutboxAt: pendingOutbox.Select(message => (DateTimeOffset?)message.CreatedAt).Min(),
                OldestReadyRunAt: oldestReadyRunAt));
        }
    }

    public ValueTask<DashboardPage<DashboardRunSummary>> GetRunsAsync(
        DashboardRunQuery query,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var normalized = query.Normalize();
        lock (_gate)
        {
            IEnumerable<JobRunRecord> runs = _runs.Values;
            if (normalized.Phase is not null)
            {
                runs = runs.Where(run => run.Phase == normalized.Phase);
            }

            if (normalized.Queue is not null)
            {
                runs = runs.Where(run => string.Equals(
                    run.Queue,
                    normalized.Queue,
                    StringComparison.Ordinal));
            }

            if (normalized.JobKey is not null)
            {
                runs = normalized.ExactJobKey
                    ? runs.Where(run => string.Equals(
                        run.JobKey,
                        normalized.JobKey,
                        StringComparison.OrdinalIgnoreCase))
                    : runs.Where(run => run.JobKey.StartsWith(
                        normalized.JobKey,
                        StringComparison.OrdinalIgnoreCase));
            }

            var ordered = runs
                .OrderByDescending(run => run.CreatedAt)
                .ThenByDescending(run => run.Id, StringComparer.Ordinal)
                .ToArray();
            var items = ordered
                .Skip((normalized.Page - 1) * normalized.PageSize)
                .Take(normalized.PageSize)
                .Select(ToDashboardSummary)
                .ToArray();

            return ValueTask.FromResult(new DashboardPage<DashboardRunSummary>(
                items,
                ordered.Length,
                normalized.Page,
                normalized.PageSize));
        }
    }

    public ValueTask<DashboardRunDetails?> GetRunDetailsAsync(
        string runId,
        bool includePayload,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult(
                _runs.TryGetValue(runId, out var run)
                    ? ToDashboardDetails(run, includePayload)
                    : null);
        }
    }

    public ValueTask<IReadOnlyList<DashboardAttemptSummary>> GetAttemptSummariesAsync(
        string runId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_attemptIdsByRun.TryGetValue(runId, out var ids))
            {
                return ValueTask.FromResult<IReadOnlyList<DashboardAttemptSummary>>(
                    Array.Empty<DashboardAttemptSummary>());
            }

            return ValueTask.FromResult<IReadOnlyList<DashboardAttemptSummary>>(
                ids.Select(id => ToDashboardAttemptSummary(_attempts[id])).ToArray());
        }
    }

    public ValueTask<IReadOnlyList<WorkerSessionRecord>> GetWorkerSessionsAsync(
        int limit,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult<IReadOnlyList<WorkerSessionRecord>>(
                _sessions.Values
                    .OrderBy(session => session.State)
                    .ThenByDescending(session => session.LastHeartbeatAt)
                    .ThenBy(session => session.WorkerId, StringComparer.Ordinal)
                    .Take(Math.Clamp(limit, 1, 1000))
                    .ToArray());
        }
    }

    public ValueTask<IReadOnlyList<JobScheduleRecord>> GetSchedulesAsync(
        int limit,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult<IReadOnlyList<JobScheduleRecord>>(
                _schedules.Values
                    .OrderByDescending(schedule => schedule.Enabled)
                    .ThenBy(schedule => schedule.NextFireAt)
                    .ThenBy(schedule => schedule.Id, StringComparer.Ordinal)
                    .Take(Math.Clamp(limit, 1, 1000))
                    .Select(CloneSchedule)
                    .ToArray());
        }
    }

    public ValueTask<IReadOnlyList<OrderingBacklogSample>> GetOrderingBacklogAsync(
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var now = DateTimeOffset.UtcNow;
            // All non-terminal runs with an ordering mode set.
            var orderedRuns = _runs.Values
                .Where(run => (run.OrderingMode == ExecutionOrderingMode.KeyOrdered
                               || run.OrderingMode == ExecutionOrderingMode.StrictFifo)
                              && !IsTerminal(run.Phase))
                .ToArray();

            var queues = orderedRuns
                .GroupBy(run => run.Queue, StringComparer.Ordinal);

            var samples = queues.Select(queueGroup =>
            {
                // KeyOrdered: group by ConcurrencyKey
                var koRuns = queueGroup
                    .Where(run => run.OrderingMode == ExecutionOrderingMode.KeyOrdered
                                  && !string.IsNullOrWhiteSpace(run.ConcurrencyKey));
                var byKey = koRuns
                    .GroupBy(run => run.ConcurrencyKey!, StringComparer.Ordinal)
                    .Select(keyGroup =>
                    {
                        var ordered = keyGroup.OrderBy(run => run.OrderingSequence).ToArray();
                        var blocked = Math.Max(0, ordered.Length - 1);
                        var oldestBlockedAge = blocked == 0
                            ? 0d
                            : ordered.Skip(1)
                                .Select(run => Math.Max(0, (now - run.AvailableAt).TotalSeconds))
                                .Max();
                        var retryBlocked = ordered.Skip(1)
                            .Count(run => ordered[0].AttemptCount > 1);
                        return (Blocked: blocked, OldestBlockedAge: oldestBlockedAge,
                                RetryBlocked: retryBlocked);
                    })
                    .ToArray();

                // StrictFifo: any inflight predecessor blocks all successors
                var sfRuns = queueGroup
                    .Where(run => run.OrderingMode == ExecutionOrderingMode.StrictFifo)
                    .OrderBy(run => run.OrderingSequence)
                    .ToArray();
                var sfBlocked = Math.Max(0, sfRuns.Length - 1);
                var sfOldestAge = sfBlocked == 0 ? 0d
                    : sfRuns.Skip(1)
                        .Select(run => Math.Max(0, (now - run.AvailableAt).TotalSeconds))
                        .Max();
                var sfRetryBlocked = sfRuns.Skip(1)
                    .Count(_ => sfRuns[0].AttemptCount > 1);

                // Per-lane breakdown (simplified: lane == 0 for in-memory default)
                var laneBreakdown = new[] { new LaneBacklogSample(
                    queueGroup.Key, LaneId: 0,
                    byKey.Sum(x => x.Blocked) + sfBlocked,
                    Math.Max(byKey.Length > 0 ? byKey.Max(x => x.OldestBlockedAge) : 0d, sfOldestAge),
                    byKey.Length,
                    sfRuns.Length > 0 ? sfRuns[0].OrderingSequence : 0) };

                return new OrderingBacklogSample(
                    queueGroup.Key,
                    byKey.Sum(x => x.Blocked),
                    byKey.Length == 0 ? 0d : byKey.Max(x => x.OldestBlockedAge),
                    byKey.Length,
                    sfBlocked,
                    byKey.Sum(x => x.RetryBlocked) + sfRetryBlocked,
                    laneBreakdown);
            })
            .ToArray();

            return ValueTask.FromResult<IReadOnlyList<OrderingBacklogSample>>(samples);
        }
    }

    private static DashboardRunSummary ToDashboardSummary(JobRunRecord run) => new()
    {
        Id = run.Id,
        JobKey = run.JobKey,
        Queue = run.Queue,
        Priority = run.Priority,
        Phase = run.Phase,
        CreatedAt = run.CreatedAt,
        StartedAt = run.StartedAt,
        CompletedAt = run.CompletedAt,
        AttemptCount = run.AttemptCount,
        MaxAttempts = run.MaxAttempts,
        ScheduleId = run.ScheduleId,
        CurrentWorkerId = run.CurrentWorkerId,
        CurrentSessionId = run.CurrentSessionId,
        CancelRequested = run.CancelRequested,
        FailureCode = run.FailureCode,
        FailureMessage = run.FailureMessage
    };

    private string? ComputeBlockedReason(JobRunRecord run)
    {
        if (run.Phase != JobPhase.Pending) return null;

        if (run.OrderingMode == ExecutionOrderingMode.KeyOrdered
            && !string.IsNullOrWhiteSpace(run.ConcurrencyKey)
            && HasOrderingPredecessor(run))
        {
            return $"KeyOrdered(predecessor inflight on key={run.ConcurrencyKey})";
        }
        if (run.OrderingMode == ExecutionOrderingMode.StrictFifo
            && HasStrictFifoPredecessor(run))
        {
            return $"StrictFifo(lane blocked by earlier run)";
        }
        return null;
    }

    private DashboardRunDetails ToDashboardDetails(
        JobRunRecord run,
        bool includePayload)
    {
        var blockedReason = (run.Phase == JobPhase.Pending && !IsTerminal(run.Phase))
            ? ComputeBlockedReason(run)
            : null;

        return new DashboardRunDetails
        {
            Id = run.Id,
            JobKey = run.JobKey,
            PayloadJson = includePayload ? run.PayloadJson : null,
            Queue = run.Queue,
            Priority = run.Priority,
            Phase = run.Phase,
            AvailableAt = run.AvailableAt,
            CreatedAt = run.CreatedAt,
            StartedAt = run.StartedAt,
            CompletedAt = run.CompletedAt,
            AttemptCount = run.AttemptCount,
            MaxAttempts = run.MaxAttempts,
            TimeoutSeconds = run.TimeoutSeconds,
            IdempotencyKey = run.IdempotencyKey,
            ConcurrencyKey = run.ConcurrencyKey,
            OrderingMode = run.OrderingMode,
            OrderingSequence = run.OrderingSequence,
            ScheduleId = run.ScheduleId,
            ScheduledFor = run.ScheduledFor,
            ParentRunId = run.ParentRunId,
            RelationKind = run.RelationKind,
            CurrentWorkerId = run.CurrentWorkerId,
            CancelRequested = run.CancelRequested,
            FailureCode = run.FailureCode,
            FailureMessage = run.FailureMessage,
            BlockedReason = blockedReason
        };
    }

    private static DashboardAttemptSummary ToDashboardAttemptSummary(
        JobAttemptRecord attempt) => new()
    {
        Id = attempt.Id,
        AttemptNumber = attempt.AttemptNumber,
        WorkerId = attempt.WorkerId,
        SessionId = attempt.SessionId,
        SessionEpoch = attempt.SessionEpoch,
        Phase = attempt.Phase,
        ClaimedAt = attempt.ClaimedAt,
        StartedAt = attempt.StartedAt,
        LeaseExpiresAt = attempt.LeaseExpiresAt,
        CompletedAt = attempt.CompletedAt,
        FailureCode = attempt.FailureCode,
        FailureMessage = attempt.FailureMessage
    };
}
