using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;

namespace KubeJob.Server.Runtime;

public sealed record ScheduleFirePlan(
    DateTimeOffset ScheduledFor,
    DateTimeOffset NextFireAt,
    bool CreateRun);

public static class ScheduleReconciliationPlanner
{
    public static ScheduleFirePlan Plan(
        JobScheduleRecord schedule,
        DateTimeOffset observedNow)
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
            return new ScheduleFirePlan(
                scheduledFor,
                CronScheduleCalculator.GetRequiredNextOccurrence(
                    schedule.CronExpression,
                    schedule.TimeZoneId,
                    now),
                schedule.MisfirePolicy == MisfirePolicy.FireOnce);
        }

        return new ScheduleFirePlan(scheduledFor, nextAfterScheduled, CreateRun: true);
    }
}
