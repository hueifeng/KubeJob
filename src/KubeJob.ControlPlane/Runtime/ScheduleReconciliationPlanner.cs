using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;

namespace KubeJob.ControlPlane.Runtime;

public sealed record ScheduleFirePlan(
    DateTimeOffset ScheduledFor,
    DateTimeOffset NextFireAt,
    bool CreateRun);

public static class ScheduleReconciliationPlanner
{
    public static ScheduleFirePlan Plan(
        JobScheduleRecord schedule,
        DateTimeOffset observedNow,
        TimeSpan misfireThreshold)
    {
        ArgumentNullException.ThrowIfNull(schedule);
        var now = observedNow.ToUniversalTime();
        var scheduledFor = schedule.NextFireAt.ToUniversalTime();
        var nextAfterScheduled = CronScheduleCalculator.GetRequiredNextOccurrence(
            schedule.CronExpression,
            schedule.TimeZoneId,
            scheduledFor);

        if (nextAfterScheduled <= now)
        {
            // More than one interval behind: a misfire. FireOnce backfills at
            // most one Run for the oldest missed occurrence, but only while
            // the miss is still within the misfire threshold; an older miss
            // (e.g. a long-disabled schedule re-enabled) is stale and is
            // skipped exactly like SkipMissed. TimeSpan.MaxValue restores the
            // unbounded backfill.
            var createRun = schedule.MisfirePolicy == MisfirePolicy.FireOnce
                && now - scheduledFor <= misfireThreshold;
            return new ScheduleFirePlan(
                scheduledFor,
                CronScheduleCalculator.GetRequiredNextOccurrence(
                    schedule.CronExpression,
                    schedule.TimeZoneId,
                    now),
                createRun);
        }

        return new ScheduleFirePlan(scheduledFor, nextAfterScheduled, CreateRun: true);
    }
}
