using System.Net;
using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace KubeJob.ApiTests;

public sealed class AttemptHistoryApiTests
{
    [Fact]
    public async Task Attempt_history_route_is_unambiguous_and_hides_lease_token()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddKubeJobServer();

        await using var app = builder.Build();
        app.MapControllers();
        await app.StartAsync();

        var store = app.Services.GetRequiredService<InMemoryJobRuntimeStore>();
        var run = (await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send",
                "{\"to\":\"user@example.com\"}",
                "default",
                0,
                DateTimeOffset.UtcNow,
                null,
                null,
                1,
                60),
            CancellationToken.None)).Run;
        var session = await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-1",
                "session-1",
                "test",
                "localhost",
                1,
                new[] { "default" },
                new[] { "mail.send" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        var claim = (await store.ClaimAsync(
            new ClaimJobsRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                1,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();

        var client = app.GetTestClient();
        using var response = await client.GetAsync($"/api/kubejob/jobs/{run.Id}/attempts");
        var json = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        json.Should().Contain(claim.AttemptId);
        json.Should().Contain("worker-1");
        json.Should().NotContain(claim.LeaseToken);
        json.Contains("leaseToken", StringComparison.OrdinalIgnoreCase).Should().BeFalse();
        json.Contains("fencingToken", StringComparison.OrdinalIgnoreCase).Should().BeFalse();
    }
}
