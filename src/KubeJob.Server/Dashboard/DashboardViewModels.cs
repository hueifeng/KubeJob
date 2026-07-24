using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Server.Dashboard;

public sealed record DashboardIndexViewModel(DashboardOverview Overview);

public sealed record DashboardRunsViewModel(
    DashboardPage<DashboardRunSummary> Runs,
    DashboardRunQuery Query);

public sealed record DashboardRunDetailsViewModel(
    JobRunRecord Run,
    IReadOnlyList<JobAttemptRecord> Attempts,
    bool ShowPayload,
    bool AllowMutatingActions);

public sealed record DashboardWorkersViewModel(
    IReadOnlyList<WorkerSessionRecord> Sessions,
    DateTimeOffset ObservedAt);

public sealed record DashboardSchedulesViewModel(
    IReadOnlyList<JobScheduleRecord> Schedules,
    bool AllowMutatingActions);
