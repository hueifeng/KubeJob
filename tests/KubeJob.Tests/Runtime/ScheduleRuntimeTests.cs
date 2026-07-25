using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class ScheduleRuntimeTests
{
    [Fact]
    public async Task Create_if_absent_does_not_overwrite_an_existing_schedule()
    {
        var store = new InMemoryJobRuntimeStore();
        var first = await store.CreateIfAbsentAsync(NewSchedule(DateTimeOffset.UtcNow), CancellationToken.None);
        var second = await store.CreateIfAbsentAsync(
            NewSchedule(DateTimeOffset.UtcNow.AddHours(1), jobKey: "other.handler"),
            CancellationToken.None);

        first.Should().NotBeNull();
        second.Should().BeNull();
        (await store.GetAsync("daily-report", CancellationToken.None))!.JobKey.Should().Be("report.generate");
    }

    [Fact]
    public async Task Expired_schedule_claim_can_be_recovered()
    {
        var store = new InMemoryJobRuntimeStore();
        var due = DateTimeOffset.UtcNow.AddMinutes(-1);
        await store.UpsertAsync(NewSchedule(due), CancellationToken.None);

        var first = await store.ClaimDueAsync(
            due.AddMinutes(2),
            TimeSpan.FromSeconds(10),
            10,
            CancellationToken.None);
        var beforeExpiry = await store.ClaimDueAsync(
            due.AddMinutes(2).AddSeconds(9),
            TimeSpan.FromSeconds(10),
            10,
            CancellationToken.None);
        var afterExpiry = await store.ClaimDueAsync(
            due.AddMinutes(2).AddSeconds(11),
            TimeSpan.FromSeconds(10),
            10,
            CancellationToken.None);

        first.Should().ContainSingle();
        beforeExpiry.Should().BeEmpty();
        afterExpiry.Should().ContainSingle();
        afterExpiry.Single().ClaimToken.Should().NotBe(first.Single().ClaimToken);
        afterExpiry.Single().ExpectedVersion.Should().BeGreaterThan(first.Single().ExpectedVersion);
    }

    [Fact]
    public async Task Commit_fire_atomically_creates_run_and_advances_schedule()
    {
        var store = new InMemoryJobRuntimeStore();
        var due = DateTimeOffset.UtcNow.AddMinutes(-1);
        await store.UpsertAsync(NewSchedule(due), CancellationToken.None);
        var claim = (await store.ClaimDueAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();
        var next = due.AddMinutes(5);

        var run = await store.CommitFireAsync(
            new CommitScheduleFireCommand(
                claim.Schedule.Id,
                claim.ClaimToken,
                claim.ExpectedVersion,
                due,
                next,
                true,
                "run-1",
                "schedule:report:1"),
            CancellationToken.None);
        var schedule = await store.GetAsync("daily-report", CancellationToken.None);
        var persistedRun = await store.GetRunAsync("run-1", CancellationToken.None);
        var outbox = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);

        run.Should().NotBeNull();
        persistedRun!.ScheduleId.Should().Be("daily-report");
        persistedRun.ScheduledFor.Should().Be(due);
        schedule!.NextFireAt.Should().Be(next);
        schedule.LastFireAt.Should().Be(due);
        outbox.Should().ContainSingle(message => message.PayloadJson.Contains("run-1"));
    }

    [Fact]
    public async Task Skip_if_running_advances_schedule_without_creating_overlapping_run()
    {
        var store = new InMemoryJobRuntimeStore();
        var due = DateTimeOffset.UtcNow.AddMinutes(-2);
        await store.UpsertAsync(
            NewSchedule(due, ScheduleConcurrencyPolicy.SkipIfRunning),
            CancellationToken.None);
        var firstClaim = (await store.ClaimDueAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();
        await store.CommitFireAsync(
            new CommitScheduleFireCommand(
                firstClaim.Schedule.Id,
                firstClaim.ClaimToken,
                firstClaim.ExpectedVersion,
                due,
                due.AddMinutes(1),
                true,
                "run-1",
                "schedule:report:1"),
            CancellationToken.None);

        var secondClaim = (await store.ClaimDueAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();
        var second = await store.CommitFireAsync(
            new CommitScheduleFireCommand(
                secondClaim.Schedule.Id,
                secondClaim.ClaimToken,
                secondClaim.ExpectedVersion,
                due.AddMinutes(1),
                due.AddMinutes(5),
                true,
                "run-2",
                "schedule:report:2"),
            CancellationToken.None);

        second.Should().BeNull();
        (await store.GetRunAsync("run-2", CancellationToken.None)).Should().BeNull();
        (await store.GetAsync("daily-report", CancellationToken.None))!
            .NextFireAt.Should().Be(due.AddMinutes(5));
    }

    [Fact]
    public async Task Updating_schedule_invalidates_an_outstanding_claim()
    {
        var store = new InMemoryJobRuntimeStore();
        var due = DateTimeOffset.UtcNow.AddMinutes(-1);
        var schedule = NewSchedule(due);
        await store.UpsertAsync(schedule, CancellationToken.None);
        var claim = (await store.ClaimDueAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();

        await store.UpsertAsync(NewSchedule(due.AddHours(1)), CancellationToken.None);
        var result = await store.CommitFireAsync(
            new CommitScheduleFireCommand(
                schedule.Id,
                claim.ClaimToken,
                claim.ExpectedVersion,
                due,
                due.AddMinutes(5),
                true,
                "stale-run",
                "schedule:stale"),
            CancellationToken.None);

        result.Should().BeNull();
        (await store.GetRunAsync("stale-run", CancellationToken.None)).Should().BeNull();
    }

    [Fact]
    public void Occurrence_id_is_deterministic_and_bounded()
    {
        var when = DateTimeOffset.Parse("2026-07-23T02:00:00Z");

        var first = ScheduleReconcilerService.CreateOccurrenceId("daily-report", when);
        var second = ScheduleReconcilerService.CreateOccurrenceId("daily-report", when);

        first.Should().Be(second);
        first.Should().HaveLength(64);
    }

    private static JobScheduleRecord NewSchedule(
        DateTimeOffset nextFireAt,
        ScheduleConcurrencyPolicy concurrencyPolicy = ScheduleConcurrencyPolicy.Allow,
        string jobKey = "report.generate") => new()
    {
        Id = "daily-report",
        JobKey = jobKey,
        PayloadJson = "{\"kind\":\"daily\"}",
        CronExpression = "*/5 * * * *",
        TimeZoneId = "UTC",
        Queue = "reports",
        Priority = 0,
        MisfirePolicy = MisfirePolicy.FireOnce,
        ConcurrencyPolicy = concurrencyPolicy,
        MaxAttempts = 3,
        TimeoutSeconds = 300,
        Enabled = true,
        NextFireAt = nextFireAt,
        CreatedAt = DateTimeOffset.UtcNow,
        UpdatedAt = DateTimeOffset.UtcNow
    };
}
