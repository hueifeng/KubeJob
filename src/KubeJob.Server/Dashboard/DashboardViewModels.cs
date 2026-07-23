using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Server.Dashboard;

public sealed record DashboardIndexViewModel(DashboardOverview Overview);

public sealed record DashboardRunsViewModel(
    DashboardPage<JobRunRecord> Runs,
    DashboardRunQuery Query);

public sealed record DashboardRunDetailsViewModel(
    JobRunRecord Run,
    IReadOnlyList<JobAttemptRecord> Attempts);

public sealed record DashboardWorkersViewModel(
    IReadOnlyList<WorkerSessionRecord> Sessions,
    DateTimeOffset ObservedAt);

public sealed record DashboardSchedulesViewModel(
    IReadOnlyList<JobScheduleRecord> Schedules);
