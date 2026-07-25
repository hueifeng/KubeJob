using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Server.Dashboard;

public sealed record DashboardIndexViewModel(DashboardOverview Overview);

public sealed record DashboardRunsViewModel(
    DashboardPage<DashboardRunSummary> Runs,
    DashboardRunQuery Query);

public sealed record DashboardRunDetailsViewModel(
    DashboardRunDetails Run,
    IReadOnlyList<DashboardAttemptSummary> Attempts,
    bool ShowPayload,
    bool AllowMutatingActions);

public sealed record DashboardWorkersViewModel(
    IReadOnlyList<WorkerSessionRecord> Sessions,
    DateTimeOffset ObservedAt,
    int MaximumItems);

public sealed record DashboardSchedulesViewModel(
    IReadOnlyList<JobScheduleRecord> Schedules,
    bool AllowMutatingActions,
    int MaximumItems);
