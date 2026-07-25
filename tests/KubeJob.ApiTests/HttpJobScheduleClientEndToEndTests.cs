using FluentAssertions;
using KubeJob.Client;
using KubeJob.Core.Jobs;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Extensions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Xunit;

namespace KubeJob.ApiTests;

public sealed record ScheduledPayload(string Kind);

public sealed class HttpJobScheduleClientEndToEndTests
{
    [Fact]
    public async Task Client_can_manage_independent_cron_schedule_over_http()
    {
        await using var app = await StartServerAsync();
        using var http = app.GetTestClient();
        var client = new HttpJobScheduleClient(http);

        var handle = await client.UpsertCronAsync(
            "daily-report",
            new JobKey<ScheduledPayload>("report.generate"),
            new ScheduledPayload("daily"),
            "0 2 * * *",
            new CronScheduleOptions
            {
                TimeZoneId = "Asia/Tokyo",
                Queue = "reports",
                MisfirePolicy = MisfirePolicy.FireOnce,
                ConcurrencyPolicy = ScheduleConcurrencyPolicy.SkipIfRunning,
                MaxAttempts = 3,
                Timeout = TimeSpan.FromMinutes(30)
            });
        var created = await client.GetAsync(handle.ScheduleId);
        var disabled = await client.SetEnabledAsync(handle.ScheduleId, false);
        var afterDisable = await client.GetAsync(handle.ScheduleId);
        var deleted = await client.DeleteAsync(handle.ScheduleId);
        var afterDelete = await client.GetAsync(handle.ScheduleId);

        created.Should().NotBeNull();
        created!.JobKey.Should().Be("report.generate");
        created.TimeZoneId.Should().Be("Asia/Tokyo");
        created.MisfirePolicy.Should().Be(MisfirePolicy.FireOnce);
        created.ConcurrencyPolicy.Should().Be(ScheduleConcurrencyPolicy.SkipIfRunning);
        disabled.Should().BeTrue();
        afterDisable!.Enabled.Should().BeFalse();
        deleted.Should().BeTrue();
        afterDelete.Should().BeNull();
    }

    private static async Task<WebApplication> StartServerAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddKubeJobServer();

        var app = builder.Build();
        app.MapControllers();
        await app.StartAsync();
        return app;
    }
}
