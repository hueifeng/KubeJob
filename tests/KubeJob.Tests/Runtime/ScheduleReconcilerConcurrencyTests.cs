using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Regression coverage for making <see cref="ScheduleReconcilerService"/> fire claimed
/// schedules with bounded concurrency instead of processing them one at a time.
/// </summary>
public sealed class ScheduleReconcilerConcurrencyTests
{
    [Fact]
    public async Task Reconciler_commits_all_due_schedules_in_one_pass_under_concurrency()
    {
        var store = new InMemoryJobRuntimeStore();
        const int scheduleCount = 12;
        var due = DateTimeOffset.UtcNow.AddMinutes(-1);
        for (var index = 0; index < scheduleCount; index++)
        {
            await store.UpsertAsync(NewSchedule($"schedule-{index}", due), CancellationToken.None);
        }

        var reconciler = new ScheduleReconcilerService(
            store,
            Options.Create(new JobRuntimeOptions
            {
                SchedulePollInterval = TimeSpan.FromMilliseconds(5),
                ScheduleClaimDuration = TimeSpan.FromSeconds(30),
                ScheduleBatchSize = scheduleCount,
                ScheduleReconcileConcurrency = 4
            }),
            NullLogger<ScheduleReconcilerService>.Instance);

        using var cancellation = new CancellationTokenSource();
        await reconciler.StartAsync(cancellation.Token);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var overview = await store.GetOverviewAsync(1, CancellationToken.None);
            if (overview.PendingOutboxMessages >= scheduleCount)
            {
                break;
            }

            await Task.Delay(10, cancellation.Token);
        }

        cancellation.Cancel();
        await reconciler.StopAsync(CancellationToken.None);

        for (var index = 0; index < scheduleCount; index++)
        {
            var schedule = await store.GetAsync($"schedule-{index}", CancellationToken.None);
            schedule.Should().NotBeNull();
            schedule!.NextFireAt.Should().BeAfter(due);
            schedule.ClaimToken.Should().BeNull();
        }

        var finalOverview = await store.GetOverviewAsync(1, CancellationToken.None);
        finalOverview.PendingOutboxMessages.Should().Be(scheduleCount);
    }

    private static JobScheduleRecord NewSchedule(
        string id,
        DateTimeOffset nextFireAt) => new()
    {
        Id = id,
        JobKey = "report.generate",
        PayloadJson = "{\"kind\":\"daily\"}",
        CronExpression = "*/5 * * * *",
        TimeZoneId = "UTC",
        Queue = "reports",
        Priority = 0,
        MisfirePolicy = MisfirePolicy.FireOnce,
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.Allow,
        MaxAttempts = 3,
        TimeoutSeconds = 300,
        Enabled = true,
        NextFireAt = nextFireAt,
        CreatedAt = DateTimeOffset.UtcNow,
        UpdatedAt = DateTimeOffset.UtcNow
    };
}
