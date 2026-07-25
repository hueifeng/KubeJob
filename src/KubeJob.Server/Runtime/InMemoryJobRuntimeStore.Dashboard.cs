using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<DashboardOverview> GetOverviewAsync(
        int recentRunCount,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var sessions = _sessions.Values.ToArray();
            var schedules = _schedules.Values.ToArray();
            var queues = _runs.Values
                .Where(run => run.Phase is JobPhase.Pending or JobPhase.Running)
                .GroupBy(run => run.Queue, StringComparer.Ordinal)
                .Select(group => new DashboardQueueSummary(
                    group.Key,
                    group.Count(run => run.Phase == JobPhase.Pending),
                    group.Count(run => run.Phase == JobPhase.Running)))
                .OrderByDescending(queue => queue.ActiveRuns)
                .ThenBy(queue => queue.Queue, StringComparer.Ordinal)
                .Take(12)
                .ToArray();
            var recent = _runs.Values
                .OrderByDescending(run => run.CreatedAt)
                .ThenByDescending(run => run.Id, StringComparer.Ordinal)
                .Take(Math.Clamp(recentRunCount, 1, 100))
                .Select(ToDashboardSummary)
                .ToArray();

            return ValueTask.FromResult(new DashboardOverview(
                PendingRuns: CountRuns(JobPhase.Pending),
                RunningRuns: CountRuns(JobPhase.Running),
                SucceededRuns: CountRuns(JobPhase.Succeeded),
                FailedRuns: CountRuns(JobPhase.Failed),
                CanceledRuns: CountRuns(JobPhase.Canceled),
                DeadRuns: CountRuns(JobPhase.Dead),
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
                PendingOutboxMessages: _outbox.Values.Count(message => message.State is
                    OutboxDeliveryState.Pending or
                    OutboxDeliveryState.Publishing or
                    OutboxDeliveryState.Failed),
                Queues: queues,
                RecentRuns: recent));
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
                runs = runs.Where(run => run.JobKey.StartsWith(
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

    private static DashboardRunDetails ToDashboardDetails(
        JobRunRecord run,
        bool includePayload) => new()
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
        ScheduleId = run.ScheduleId,
        ScheduledFor = run.ScheduledFor,
        CurrentWorkerId = run.CurrentWorkerId,
        CancelRequested = run.CancelRequested,
        FailureCode = run.FailureCode,
        FailureMessage = run.FailureMessage
    };

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

    private int CountRuns(JobPhase phase) =>
        _runs.Values.Count(run => run.Phase == phase);
}
