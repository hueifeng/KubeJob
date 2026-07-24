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

    public ValueTask<DashboardPage<JobRunRecord>> GetRunsAsync(
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
                runs = runs.Where(run => run.JobKey.Contains(
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
                .ToArray();

            return ValueTask.FromResult(new DashboardPage<JobRunRecord>(
                items,
                ordered.Length,
                normalized.Page,
                normalized.PageSize));
        }
    }

    public ValueTask<IReadOnlyList<WorkerSessionRecord>> GetWorkerSessionsAsync(
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
                    .ToArray());
        }
    }

    public ValueTask<IReadOnlyList<JobScheduleRecord>> GetSchedulesAsync(
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
                    .Select(CloneSchedule)
                    .ToArray());
        }
    }

    private int CountRuns(JobPhase phase) =>
        _runs.Values.Count(run => run.Phase == phase);
}
