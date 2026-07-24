using FluentAssertions;
using KubeJob.Client;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;
using KubeJob.Server.Extensions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Xunit;

namespace KubeJob.ApiTests;

public sealed record RemotePayload(string Value);

public sealed class HttpJobClientEndToEndTests
{
    [Fact]
    public async Task Client_can_enqueue_query_and_cancel_over_http()
    {
        await using var app = await StartServerAsync();
        using var http = app.GetTestClient();
        var client = new HttpJobClient(http);

        var handle = await client.EnqueueAsync(
            new JobKey<RemotePayload>("remote.echo"),
            new RemotePayload("hello"),
            new JobEnqueueOptions
            {
                Queue = "default",
                IdempotencyKey = "remote:1"
            });
        var pending = await client.GetStatusAsync(handle.JobId);
        var canceled = await client.CancelAsync(handle.JobId, "test cancellation");
        var terminal = await client.GetStatusAsync(handle.JobId);

        pending.Should().NotBeNull();
        pending!.Phase.Should().Be(JobPhase.Pending);
        canceled.Should().BeTrue();
        terminal!.Phase.Should().Be(JobPhase.Canceled);
    }

    [Fact]
    public async Task Client_surfaces_idempotency_conflict_from_server()
    {
        await using var app = await StartServerAsync();
        using var http = app.GetTestClient();
        var client = new HttpJobClient(http);
        var key = new JobKey<RemotePayload>("remote.echo");
        var options = new JobEnqueueOptions
        {
            IdempotencyKey = "remote:conflict"
        };

        var first = await client.EnqueueAsync(
            key,
            new RemotePayload("first"),
            options);
        var action = async () => await client.EnqueueAsync(
            key,
            new RemotePayload("different"),
            options);

        var exception = await action.Should().ThrowAsync<IdempotencyConflictException>();
        exception.Which.IdempotencyKey.Should().Be("remote:conflict");
        exception.Which.ExistingJobId.Should().Be(first.JobId);
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
