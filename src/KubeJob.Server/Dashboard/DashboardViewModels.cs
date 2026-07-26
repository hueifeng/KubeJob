using System.ComponentModel.DataAnnotations;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Runtime;

namespace KubeJob.Server.Dashboard;

public sealed record DashboardIndexViewModel(DashboardOverview Overview);

public sealed record DashboardRunsViewModel(
    DashboardPage<DashboardRunSummary> Runs,
    DashboardRunQuery Query,
    DashboardOverview Overview);

public sealed record DashboardFailuresViewModel(
    DashboardPage<DashboardRunSummary> PermanentFailures,
    DashboardPage<DashboardRunSummary> ExhaustedRetries,
    DashboardRunQuery PermanentFailureQuery,
    DashboardRunQuery ExhaustedRetryQuery,
    DashboardOverview Overview);

public sealed record DashboardTimelineEvent(
    DateTimeOffset At,
    string Title,
    string Detail,
    string Tone);

public sealed record DashboardRunDetailsViewModel(
    DashboardRunDetails Run,
    IReadOnlyList<DashboardAttemptSummary> Attempts,
    IReadOnlyList<DashboardTimelineEvent> Timeline,
    bool ShowPayload,
    bool AllowMutatingActions)
{
    public static DashboardRunDetailsViewModel Create(
        DashboardRunDetails run,
        IReadOnlyList<DashboardAttemptSummary> attempts,
        bool showPayload,
        bool allowMutatingActions)
    {
        var timeline = new List<DashboardTimelineEvent>
        {
            new(
                run.CreatedAt,
                "Job submitted",
                $"Queued as {run.JobKey} on {run.Queue} with priority {run.Priority}.",
                "neutral")
        };

        if (run.AvailableAt > run.CreatedAt && attempts.Count == 0)
        {
            timeline.Add(new DashboardTimelineEvent(
                run.AvailableAt,
                "Job became eligible to run",
                "The configured delay elapsed and a matching worker could claim the job.",
                "primary"));
        }

        foreach (var attempt in attempts.OrderBy(item => item.AttemptNumber))
        {
            timeline.Add(new DashboardTimelineEvent(
                attempt.ClaimedAt,
                $"Attempt {attempt.AttemptNumber} claimed",
                $"Worker {attempt.WorkerId} claimed the job using session {attempt.SessionId}, epoch {attempt.SessionEpoch}.",
                "primary"));

            if (attempt.StartedAt > attempt.ClaimedAt)
            {
                timeline.Add(new DashboardTimelineEvent(
                    attempt.StartedAt,
                    $"Attempt {attempt.AttemptNumber} started",
                    $"Worker {attempt.WorkerId} began executing the handler.",
                    "primary"));
            }

            if (attempt.CompletedAt is not { } completedAt)
            {
                continue;
            }

            var (title, detail, tone) = attempt.Phase switch
            {
                JobAttemptPhase.Succeeded => (
                    $"Attempt {attempt.AttemptNumber} succeeded",
                    "The worker completed the handler and the result was accepted.",
                    "success"),
                JobAttemptPhase.RetryableFailure => (
                    $"Attempt {attempt.AttemptNumber} failed; retry allowed",
                    FormatFailure(attempt),
                    "warning"),
                JobAttemptPhase.PermanentFailure => (
                    $"Attempt {attempt.AttemptNumber} permanently failed",
                    FormatFailure(attempt),
                    "danger"),
                JobAttemptPhase.Canceled => (
                    $"Attempt {attempt.AttemptNumber} canceled",
                    FormatFailure(attempt, "The attempt stopped after cancellation was requested."),
                    "neutral"),
                JobAttemptPhase.TimedOut => (
                    $"Attempt {attempt.AttemptNumber} timed out",
                    FormatFailure(attempt),
                    "warning"),
                JobAttemptPhase.LeaseLost => (
                    $"Attempt {attempt.AttemptNumber} lost its lease",
                    FormatFailure(attempt),
                    "warning"),
                JobAttemptPhase.Rejected => (
                    $"Attempt {attempt.AttemptNumber} result rejected",
                    FormatFailure(attempt),
                    "danger"),
                _ => (
                    $"Attempt {attempt.AttemptNumber} finished as {attempt.Phase}",
                    FormatFailure(attempt, "The attempt reached a terminal state."),
                    "neutral")
            };

            timeline.Add(new DashboardTimelineEvent(completedAt, title, detail, tone));
        }

        var latestUnsuccessfulCompletion = attempts
            .Where(item => item.Phase is
                JobAttemptPhase.RetryableFailure or
                JobAttemptPhase.TimedOut or
                JobAttemptPhase.LeaseLost)
            .Select(item => item.CompletedAt)
            .Where(item => item is not null)
            .Max();
        if (run.Phase == JobPhase.Pending
            && latestUnsuccessfulCompletion is { } retryFrom
            && run.AvailableAt >= retryFrom)
        {
            timeline.Add(new DashboardTimelineEvent(
                run.AvailableAt,
                "Retry scheduled",
                $"The next Attempt becomes claimable after {run.AvailableAt.ToLocalTime():yyyy-MM-dd HH:mm:ss zzz}.",
                "warning"));
        }

        return new DashboardRunDetailsViewModel(
            run,
            attempts,
            timeline
                .OrderBy(item => item.At)
                .ThenBy(item => item.Title, StringComparer.Ordinal)
                .ToArray(),
            showPayload,
            allowMutatingActions);
    }

    private static string FormatFailure(
        DashboardAttemptSummary attempt,
        string fallback = "The worker reported an unsuccessful result.")
    {
        if (attempt.FailureCode is null)
        {
            return fallback;
        }

        return string.IsNullOrWhiteSpace(attempt.FailureMessage)
            ? attempt.FailureCode
            : $"{attempt.FailureCode}: {attempt.FailureMessage}";
    }
}

public sealed record DashboardWorkersViewModel(
    IReadOnlyList<WorkerSessionRecord> Sessions,
    DateTimeOffset ObservedAt,
    int MaximumItems,
    bool IncludeHistory,
    int ActiveSessionCount,
    int HistoricalSessionCount);

public sealed record DashboardSchedulesViewModel(
    IReadOnlyList<JobScheduleRecord> Schedules,
    bool AllowMutatingActions,
    int MaximumItems,
    DashboardScheduleCreateForm CreateForm,
    bool ShowCreateForm,
    IReadOnlyList<string> ReadyJobKeys);

public sealed class DashboardScheduleCreateForm
{
    [Required]
    [StringLength(200)]
    public string Id { get; set; } = string.Empty;

    [Required]
    [StringLength(300)]
    public string JobKey { get; set; } = string.Empty;

    [Required]
    [StringLength(1_000_000)]
    public string PayloadJson { get; set; } = "{\"Message\":\"recurring demo\",\"Steps\":3}";

    [Required]
    [StringLength(200)]
    public string CronExpression { get; set; } = "* * * * *";

    [Required]
    [StringLength(200)]
    public string TimeZoneId { get; set; } = "UTC";

    [Required]
    [StringLength(100)]
    public string Queue { get; set; } = "default";

    [Range(-1000, 1000)]
    public int Priority { get; set; }

    public MisfirePolicy MisfirePolicy { get; set; } = MisfirePolicy.FireOnce;

    public ScheduleConcurrencyPolicy ConcurrencyPolicy { get; set; } = ScheduleConcurrencyPolicy.SkipIfRunning;

    [Range(1, 100)]
    public int MaxAttempts { get; set; } = 1;

    [Range(1, 86400)]
    public int TimeoutSeconds { get; set; } = 300;

    public bool Enabled { get; set; } = true;
}
