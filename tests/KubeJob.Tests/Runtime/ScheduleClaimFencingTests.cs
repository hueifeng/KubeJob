using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class ScheduleClaimFencingTests
{
    [Fact]
    public async Task Expired_claim_cannot_commit_an_occurrence()
    {
        var store = new InMemoryJobRuntimeStore();
        var due = DateTimeOffset.UtcNow.AddMinutes(-1);
        await store.UpsertAsync(NewSchedule(due), CancellationToken.None);
        var claim = (await store.ClaimDueAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromMilliseconds(10),
            1,
            CancellationToken.None)).Single();

        await Task.Delay(50);
        var result = await store.CommitFireAsync(
            Fire(claim, due, "expired-run"),
            CancellationToken.None);

        result.Should().BeNull();
        (await store.GetRunAsync("expired-run", CancellationToken.None)).Should().BeNull();
    }

    [Fact]
    public async Task Released_claim_is_immediately_fenced()
    {
        var store = new InMemoryJobRuntimeStore();
        var due = DateTimeOffset.UtcNow.AddMinutes(-1);
        await store.UpsertAsync(NewSchedule(due), CancellationToken.None);
        var claim = (await store.ClaimDueAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();

        await store.ReleaseClaimAsync(
            claim.Schedule.Id,
            claim.ClaimToken,
            DateTimeOffset.UtcNow.AddMinutes(1),
            CancellationToken.None);
        var result = await store.CommitFireAsync(
            Fire(claim, due, "released-run"),
            CancellationToken.None);

        result.Should().BeNull();
        (await store.GetRunAsync("released-run", CancellationToken.None)).Should().BeNull();
    }

    private static CommitScheduleFireCommand Fire(
        ClaimedSchedule claim,
        DateTimeOffset scheduledFor,
        string runId) => new(
        claim.Schedule.Id,
        claim.ClaimToken,
        claim.ExpectedVersion,
        scheduledFor,
        scheduledFor.AddMinutes(5),
        true,
        runId,
        $"schedule:{runId}");

    private static JobScheduleRecord NewSchedule(DateTimeOffset nextFireAt) => new()
    {
        Id = "fenced-schedule",
        JobKey = "report.generate",
        PayloadJson = "{}",
        CronExpression = "*/5 * * * *",
        TimeZoneId = "UTC",
        Queue = "reports",
        MisfirePolicy = MisfirePolicy.FireOnce,
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.Allow,
        MaxAttempts = 1,
        TimeoutSeconds = 60,
        Enabled = true,
        NextFireAt = nextFireAt,
        CreatedAt = DateTimeOffset.UtcNow,
        UpdatedAt = DateTimeOffset.UtcNow
    };
}
