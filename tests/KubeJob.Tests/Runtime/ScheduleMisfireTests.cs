using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class ScheduleMisfireTests
{
    [Fact]
    public void Fire_once_creates_one_run_after_multiple_missed_occurrences()
    {
        var now = DateTimeOffset.Parse("2026-07-23T12:00:30Z");
        var schedule = NewSchedule(
            nextFireAt: now.AddMinutes(-10),
            MisfirePolicy.FireOnce);

        var plan = ScheduleReconciliationPlanner.Plan(schedule, now, TimeSpan.FromHours(1));

        plan.CreateRun.Should().BeTrue();
        plan.ScheduledFor.Should().Be(schedule.NextFireAt);
        plan.NextFireAt.Should().BeAfter(now);
    }

    [Fact]
    public void Fire_once_skips_occurrences_missed_beyond_the_misfire_threshold()
    {
        var now = DateTimeOffset.Parse("2026-07-23T12:00:30Z");
        var schedule = NewSchedule(
            nextFireAt: now.AddHours(-2),
            MisfirePolicy.FireOnce);

        var plan = ScheduleReconciliationPlanner.Plan(schedule, now, TimeSpan.FromHours(1));

        plan.CreateRun.Should().BeFalse();
        plan.NextFireAt.Should().BeAfter(now);
    }

    [Fact]
    public void Fire_once_backfills_without_bound_when_the_threshold_is_infinite()
    {
        var now = DateTimeOffset.Parse("2026-07-23T12:00:30Z");
        var schedule = NewSchedule(
            nextFireAt: now.AddHours(-2),
            MisfirePolicy.FireOnce);

        var plan = ScheduleReconciliationPlanner.Plan(schedule, now, TimeSpan.MaxValue);

        plan.CreateRun.Should().BeTrue();
        plan.ScheduledFor.Should().Be(schedule.NextFireAt);
        plan.NextFireAt.Should().BeAfter(now);
    }

    [Fact]
    public void Fire_once_never_backfills_when_the_threshold_is_zero()
    {
        var now = DateTimeOffset.Parse("2026-07-23T12:00:30Z");
        var schedule = NewSchedule(
            nextFireAt: now.AddMinutes(-10),
            MisfirePolicy.FireOnce);

        var plan = ScheduleReconciliationPlanner.Plan(schedule, now, TimeSpan.Zero);

        plan.CreateRun.Should().BeFalse();
        plan.NextFireAt.Should().BeAfter(now);
    }

    [Fact]
    public void Skip_missed_advances_without_creating_a_run_after_multiple_misses()
    {
        var now = DateTimeOffset.Parse("2026-07-23T12:00:30Z");
        var schedule = NewSchedule(
            nextFireAt: now.AddMinutes(-10),
            MisfirePolicy.SkipMissed);

        var plan = ScheduleReconciliationPlanner.Plan(schedule, now, TimeSpan.FromHours(1));

        plan.CreateRun.Should().BeFalse();
        plan.NextFireAt.Should().BeAfter(now);
    }

    [Fact]
    public void Skip_missed_still_fires_the_current_due_occurrence()
    {
        var now = DateTimeOffset.Parse("2026-07-23T12:00:30Z");
        var schedule = NewSchedule(
            nextFireAt: DateTimeOffset.Parse("2026-07-23T12:00:00Z"),
            MisfirePolicy.SkipMissed);

        var plan = ScheduleReconciliationPlanner.Plan(schedule, now, TimeSpan.FromHours(1));

        plan.CreateRun.Should().BeTrue();
        plan.NextFireAt.Should().Be(DateTimeOffset.Parse("2026-07-23T12:01:00Z"));
    }

    private static JobScheduleRecord NewSchedule(
        DateTimeOffset nextFireAt,
        MisfirePolicy misfirePolicy) => new()
    {
        Id = "minute-report",
        JobKey = "report.generate",
        PayloadJson = "{}",
        CronExpression = "* * * * *",
        TimeZoneId = "UTC",
        Queue = "reports",
        MisfirePolicy = misfirePolicy,
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.Allow,
        MaxAttempts = 1,
        TimeoutSeconds = 60,
        Enabled = true,
        NextFireAt = nextFireAt,
        CreatedAt = nextFireAt,
        UpdatedAt = nextFireAt
    };
}
