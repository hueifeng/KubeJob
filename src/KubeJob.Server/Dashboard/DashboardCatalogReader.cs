using KubeJob.Core.Scheduling;
using KubeJob.Core.Runtime;
using KubeJob.Server.Options;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Server.Dashboard;

/// <summary>
/// Builds the live, presentation-safe catalog shared by the Job keys and
/// Schedules pages. It keeps the worker, run, and schedule correlation out of
/// controllers and views.
/// </summary>
public sealed class DashboardCatalogReader
{
    private const int RecentRunLimit = 100;

    private readonly IJobRuntimeDashboardStore _dashboard;
    private readonly KubeJobDashboardOptions _options;

    public DashboardCatalogReader(
        IJobRuntimeDashboardStore dashboard,
        KubeJobDashboardOptions options)
    {
        _dashboard = dashboard;
        _options = options;
    }

    public async ValueTask<DashboardJobCatalog> ReadAsync(CancellationToken cancellationToken)
    {
        var sessionsTask = _dashboard
            .GetWorkerSessionsAsync(_options.GetNormalizedMaximumWorkerSessions(), cancellationToken)
            .AsTask();
        var runsTask = _dashboard
            .GetRunsAsync(new DashboardRunQuery(PageSize: RecentRunLimit), cancellationToken)
            .AsTask();
        var schedulesTask = _dashboard
            .GetSchedulesAsync(_options.GetNormalizedMaximumSchedules(), cancellationToken)
            .AsTask();

        await Task.WhenAll(sessionsTask, runsTask, schedulesTask);

        var sessions = await sessionsTask;
        var recentRuns = await runsTask;
        var schedules = await schedulesTask;
        var summaries = sessions
            .SelectMany(session => session.Capabilities)
            .Concat(recentRuns.Items.Select(run => run.JobKey))
            .Concat(schedules.Select(schedule => schedule.JobKey))
            .Where(key => !string.IsNullOrWhiteSpace(key))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(key => key, StringComparer.Ordinal)
            .Select(jobKey => CreateSummary(jobKey, sessions, recentRuns.Items, schedules))
            .ToArray();

        return new DashboardJobCatalog(
            summaries,
            schedules,
            DateTimeOffset.UtcNow,
            RecentRunLimit);
    }

    private static DashboardJobTypeSummary CreateSummary(
        string jobKey,
        IReadOnlyList<WorkerSessionRecord> sessions,
        IReadOnlyList<DashboardRunSummary> recentRuns,
        IReadOnlyList<JobScheduleRecord> schedules)
    {
        var matchingSessions = sessions
            .Where(session => session.Capabilities.Contains(jobKey, StringComparer.Ordinal))
            .ToArray();
        var matchingRuns = recentRuns
            .Where(run => string.Equals(run.JobKey, jobKey, StringComparison.Ordinal))
            .OrderByDescending(run => run.CreatedAt)
            .ToArray();
        var latestRun = matchingRuns.FirstOrDefault();
        var matchingSchedules = schedules
            .Where(schedule => string.Equals(schedule.JobKey, jobKey, StringComparison.Ordinal))
            .ToArray();

        return new DashboardJobTypeSummary(
            jobKey,
            matchingSessions.Select(session => session.WorkerId).Distinct(StringComparer.Ordinal).Count(),
            matchingSessions
                .Where(session => session.State == WorkerSessionState.Ready)
                .Select(session => session.WorkerId)
                .Distinct(StringComparer.Ordinal)
                .Count(),
            matchingSessions
                .Select(session => session.WorkerId)
                .Distinct(StringComparer.Ordinal)
                .OrderBy(workerId => workerId, StringComparer.Ordinal)
                .ToArray(),
            matchingSessions
                .SelectMany(session => session.Queues)
                .Distinct(StringComparer.Ordinal)
                .OrderBy(queue => queue, StringComparer.Ordinal)
                .ToArray(),
            matchingRuns.Length,
            latestRun?.CreatedAt,
            latestRun?.Phase,
            matchingSchedules.Length);
    }
}

public sealed record DashboardJobCatalog(
    IReadOnlyList<DashboardJobTypeSummary> JobTypes,
    IReadOnlyList<JobScheduleRecord> Schedules,
    DateTimeOffset ObservedAt,
    int RecentRunLimit)
{
    public IReadOnlyList<string> ReadyJobKeys => JobTypes
        .Where(jobType => jobType.IsAvailable)
        .Select(jobType => jobType.JobKey)
        .ToArray();
}
