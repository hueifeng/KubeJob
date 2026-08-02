using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Server.Dashboard;

/// <summary>
/// Turns a runtime snapshot into operator-facing state so Dashboard views do not
/// need to duplicate thresholds or infer whether delayed work is claimable.
/// </summary>
public sealed record DashboardOperationalSummary(
    bool IsIdle,
    bool WaitingWithoutCapacity,
    bool HasAgingBacklog,
    bool HasRecentFailures,
    bool HasOutboxBacklog,
    TimeSpan? OldestReadyAge,
    DashboardOperationalMessage Message,
    bool HasFailedOutbox = false)
{
    public bool HasAttention =>
        WaitingWithoutCapacity || HasAgingBacklog || HasRecentFailures || HasFailedOutbox;

    public static DashboardOperationalSummary Create(DashboardOverview overview)
    {
        var activeRuns = overview.PendingRuns + overview.RunningRuns;
        var oldestReadyAt = overview.OldestReadyRunAt ?? overview.Queues
            .Where(queue => queue.OldestReadyAt is not null)
            .Select(queue => queue.OldestReadyAt)
            .OrderBy(value => value)
            .FirstOrDefault();
        TimeSpan? oldestReadyAge = oldestReadyAt is null
            ? null
            : overview.ObservedAt - oldestReadyAt.Value;

        // A delayed retry remains Pending but is not ready to claim; it must not
        // be diagnosed as a capacity outage before it becomes eligible.
        var waitingWithoutCapacity = oldestReadyAge is not null
            && overview.AvailableWorkerSlots == 0
            && (overview.ReadyWorkers == 0 || oldestReadyAge >= TimeSpan.FromMinutes(1));
        var hasAgingBacklog = oldestReadyAge >= TimeSpan.FromMinutes(5);
        var hasRecentFailures = overview.LastHour.UnsuccessfulRuns > 0;
        var hasOutboxBacklog = overview.PendingOutboxMessages > 0;
        var hasFailedOutbox = overview.FailedOutboxMessages > 0;
        var isIdle = activeRuns == 0
            && overview.LastHour.CompletedRuns == 0
            && overview.LastHour.UnsuccessfulRuns == 0
            && overview.Queues.Count == 0
            && !hasOutboxBacklog;

        var message = waitingWithoutCapacity && overview.ReadyWorkers == 0
            ? new DashboardOperationalMessage(
                "danger", "!", "Ready jobs are waiting, but no worker is ready.",
                "Start or restore a worker that listens to the affected queue before the backlog grows.",
                "Check workers", "Workers")
            : hasAgingBacklog
                ? new DashboardOperationalMessage(
                    "warning", "◷", $"The oldest ready job has waited {FormatAge(oldestReadyAge!.Value)}.",
                    "Check whether workers cover that queue and still have available execution slots.",
                    "Check workers", "Workers")
                : hasRecentFailures
                    ? new DashboardOperationalMessage(
                        "warning", "!", $"{overview.LastHour.UnsuccessfulRuns} job(s) failed or exhausted retries in the last hour.",
                        "Open the job list to inspect the latest error and complete Attempt history.",
                        "Review failures", "Failures")
                    : hasFailedOutbox
                    ? new DashboardOperationalMessage(
                        "danger", "!", "Outbox publication is failing.",
                        $"{overview.FailedOutboxMessages} message(s) are in retry state. The oldest has waited {FormatAge(overview.ObservedAt - overview.OldestPendingOutboxAt.GetValueOrDefault(overview.ObservedAt))}.",
                        "Review failures", "Failures")
                    : hasOutboxBacklog
                        ? new DashboardOperationalMessage(
                            "neutral", "◌", "Outbox delivery is pending.",
                            $"{overview.PendingOutboxMessages} notification message(s) await publication or retry. Stored job state is unaffected.",
                            null, null)
                        : activeRuns > 0
                            ? new DashboardOperationalMessage(
                                "success", "✓", "Jobs are moving through the runtime.",
                                $"{overview.PendingRuns} waiting, {overview.RunningRuns} running, and {overview.AvailableWorkerSlots} worker slot(s) currently available.",
                                "View jobs", "Runs")
                            : new DashboardOperationalMessage(
                                "neutral", "—", "Runtime ready — no work is waiting.",
                                $"{overview.ReadyWorkers} ready worker(s) provide {overview.AvailableWorkerSlots} available slot(s). New jobs will appear after your application submits them.",
                                null, null);

        return new DashboardOperationalSummary(
            isIdle,
            waitingWithoutCapacity,
            hasAgingBacklog,
            hasRecentFailures,
            hasOutboxBacklog,
            oldestReadyAge,
            message,
            hasFailedOutbox);
    }

    public static string FormatAge(TimeSpan value)
    {
        var age = value < TimeSpan.Zero ? TimeSpan.Zero : value;
        if (age.TotalDays >= 1)
        {
            return $"{(int)age.TotalDays}d {age.Hours}h";
        }

        if (age.TotalHours >= 1)
        {
            return $"{(int)age.TotalHours}h {age.Minutes}m";
        }

        return age.TotalMinutes >= 1
            ? $"{(int)age.TotalMinutes}m {age.Seconds}s"
            : $"{Math.Floor(age.TotalSeconds):0}s";
    }
}

public sealed record DashboardOperationalMessage(
    string Tone,
    string Icon,
    string Title,
    string Detail,
    string? Action,
    string? ActionRoute);
