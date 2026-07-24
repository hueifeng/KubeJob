using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public sealed record DashboardPage<T>(
    IReadOnlyList<T> Items,
    int TotalCount,
    int Page,
    int PageSize)
{
    public int TotalPages => TotalCount == 0
        ? 0
        : (int)Math.Ceiling(TotalCount / (double)PageSize);
}

public sealed record DashboardRunQuery(
    int Page = 1,
    int PageSize = 25,
    JobPhase? Phase = null,
    string? Queue = null,
    string? JobKey = null)
{
    public DashboardRunQuery Normalize()
    {
        var page = Math.Max(1, Page);
        var pageSize = Math.Clamp(PageSize, 10, 100);
        return this with
        {
            Page = page,
            PageSize = pageSize,
            Queue = string.IsNullOrWhiteSpace(Queue) ? null : Queue.Trim(),
            JobKey = string.IsNullOrWhiteSpace(JobKey) ? null : JobKey.Trim()
        };
    }
}

public sealed record DashboardQueueSummary(
    string Queue,
    int PendingRuns,
    int RunningRuns)
{
    public int ActiveRuns => PendingRuns + RunningRuns;
}

public sealed record DashboardOverview(
    int PendingRuns,
    int RunningRuns,
    int SucceededRuns,
    int FailedRuns,
    int CanceledRuns,
    int DeadRuns,
    int ReadyWorkers,
    int DrainingWorkers,
    int TotalWorkerCapacity,
    int AvailableWorkerSlots,
    int EnabledSchedules,
    int DisabledSchedules,
    int PendingOutboxMessages,
    IReadOnlyList<DashboardQueueSummary> Queues,
    IReadOnlyList<JobRunRecord> RecentRuns);

public interface IJobRuntimeDashboardStore
{
    ValueTask<DashboardOverview> GetOverviewAsync(
        int recentRunCount,
        CancellationToken cancellationToken);

    ValueTask<DashboardPage<JobRunRecord>> GetRunsAsync(
        DashboardRunQuery query,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<WorkerSessionRecord>> GetWorkerSessionsAsync(
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<JobScheduleRecord>> GetSchedulesAsync(
        CancellationToken cancellationToken);
}
