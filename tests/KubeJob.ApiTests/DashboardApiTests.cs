using System.Net;
using System.Security.Claims;
using System.Text.Encodings.Web;
using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace KubeJob.ApiTests;

public sealed class DashboardApiTests
{
    [Fact]
    public async Task Dashboard_uses_custom_route_scoped_authorization_and_safe_defaults()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services
            .AddAuthentication("dashboard-test")
            .AddScheme<AuthenticationSchemeOptions, HeaderAuthenticationHandler>(
                "dashboard-test",
                _ => { });
        builder.Services.AddAuthorization(options =>
        {
            options.AddPolicy("KubeJobDashboard", policy =>
            {
                policy.AddAuthenticationSchemes("dashboard-test");
                policy.RequireAuthenticatedUser();
            });
        });
        builder.Services.AddKubeJobServer();
        builder.Services.AddKubeJobDashboard(options =>
        {
            options.RoutePrefix = "/admin/jobs/";
            options.AuthorizationPolicy = "KubeJobDashboard";
        });

        await using var app = builder.Build();
        app.UseRouting();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapControllers();
        await app.StartAsync();

        var store = app.Services.GetRequiredService<InMemoryJobRuntimeStore>();
        var permanentRun = (await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send",
                "{\"apiKey\":\"top-secret-value\"}",
                "mail",
                0,
                DateTimeOffset.UtcNow,
                null,
                null,
                1,
                60),
            CancellationToken.None)).Run;

        var client = app.GetTestClient();

        using var anonymousResponse = await client.GetAsync("/admin/jobs");
        anonymousResponse.StatusCode.Should().Be(HttpStatusCode.Unauthorized);

        using var overviewRequest = CreateAuthorizedRequest("/admin/jobs");
        using var overviewResponse = await client.SendAsync(overviewRequest);
        var overviewHtml = await overviewResponse.Content.ReadAsStringAsync();

        overviewResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        overviewHtml.Should().Contain("Runtime Overview");
        overviewHtml.Should().Contain("<span class=\"label\">Jobs</span>");
        overviewHtml.Should().Contain("<span class=\"label\">Failures</span>");
        overviewHtml.Should().Contain("Read-only dashboard");
        overviewHtml.Should().Contain("aria-label=\"Dashboard navigation\"");
        overviewHtml.Should().Contain("aria-current=\"page\"");
        overviewHtml.Should().Contain("Jobs are waiting, but no worker is ready.");
        overviewHtml.Should().Contain("Check workers");
        overviewHtml.Should().Contain("Jobs in progress");
        overviewHtml.Should().Contain("Run</strong> means one logical job");
        overviewHtml.Should().NotContain("Control plane online");
        overviewHtml.Contains("LeaseToken", StringComparison.OrdinalIgnoreCase).Should().BeFalse();
        overviewHtml.Should().NotContain("cdn.jsdelivr.net");
        overviewHtml.Should().NotContain("cdnjs.cloudflare.com");

        using var initialDetailRequest = CreateAuthorizedRequest($"/admin/jobs/runs/{permanentRun.Id}");
        using var initialDetailResponse = await client.SendAsync(initialDetailRequest);
        var initialDetailHtml = await initialDetailResponse.Content.ReadAsStringAsync();

        initialDetailResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        initialDetailHtml.Should().Contain("Job Details");
        initialDetailHtml.Should().Contain("A Run is one logical job");
        initialDetailHtml.Should().Contain("Execution timeline");
        initialDetailHtml.Should().Contain("Job submitted");
        initialDetailHtml.Should().Contain("Payload display is disabled");
        initialDetailHtml.Should().NotContain("top-secret-value");
        initialDetailHtml.Contains("LeaseToken", StringComparison.OrdinalIgnoreCase).Should().BeFalse();
        initialDetailHtml.Should().NotContain("Request cancellation");

        var exhaustedRun = (await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send",
                "{}",
                "mail",
                0,
                DateTimeOffset.UtcNow,
                null,
                null,
                1,
                60),
            CancellationToken.None)).Run;
        var session = await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-dashboard",
                "session-dashboard",
                "test",
                "localhost",
                2,
                new[] { "mail" },
                new[] { "mail.send" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        var claims = await store.ClaimAsync(
            new ClaimJobsRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                2,
                new[] { "mail" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            2,
            CancellationToken.None);
        var permanentClaim = claims.Single(item => item.RunId == permanentRun.Id);
        var exhaustedClaim = claims.Single(item => item.RunId == exhaustedRun.Id);

        var permanentCompletion = await store.CompleteAsync(
            new CompleteAttemptRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                permanentClaim.RunId,
                permanentClaim.AttemptId,
                permanentClaim.AttemptNumber,
                permanentClaim.LeaseToken,
                JobAttemptOutcome.PermanentFailure,
                "smtp_rejected",
                "The recipient was rejected."),
            TimeSpan.Zero,
            CancellationToken.None);
        var exhaustedCompletion = await store.CompleteAsync(
            new CompleteAttemptRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                exhaustedClaim.RunId,
                exhaustedClaim.AttemptId,
                exhaustedClaim.AttemptNumber,
                exhaustedClaim.LeaseToken,
                JobAttemptOutcome.RetryableFailure,
                "socket_timeout",
                "The upstream service did not respond."),
            TimeSpan.Zero,
            CancellationToken.None);

        permanentCompletion.Phase.Should().Be(JobPhase.Failed);
        exhaustedCompletion.Phase.Should().Be(JobPhase.Dead);

        using var jobsRequest = CreateAuthorizedRequest("/admin/jobs/runs");
        using var jobsResponse = await client.SendAsync(jobsRequest);
        var jobsHtml = await jobsResponse.Content.ReadAsStringAsync();

        jobsResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        jobsHtml.Should().Contain("Review failures");
        jobsHtml.Should().Contain("Permanent failures");
        jobsHtml.Should().Contain("No retries left");
        jobsHtml.Should().Contain("Canceled");
        jobsHtml.Should().Contain("smtp_rejected");
        jobsHtml.Should().Contain("socket_timeout");

        using var scopedJobsRequest = CreateAuthorizedRequest("/admin/jobs/runs?queue=mail&phase=Failed");
        using var scopedJobsResponse = await client.SendAsync(scopedJobsRequest);
        var scopedJobsHtml = await scopedJobsResponse.Content.ReadAsStringAsync();

        scopedJobsResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        scopedJobsHtml.Should().Contain("A count is shown only for the currently selected status");
        scopedJobsHtml.Should().Contain("smtp_rejected");
        scopedJobsHtml.Should().NotContain("socket_timeout");

        using var failuresRequest = CreateAuthorizedRequest("/admin/jobs/failures?queue=mail");
        using var failuresResponse = await client.SendAsync(failuresRequest);
        var failuresHtml = await failuresResponse.Content.ReadAsStringAsync();

        failuresResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        failuresHtml.Should().Contain("Failure Workbench");
        failuresHtml.Should().Contain("Summary counts and both tables reflect the active Queue and Job key filters");
        failuresHtml.Should().Contain("Permanent failures");
        failuresHtml.Should().Contain("No retries left");
        failuresHtml.Should().Contain("smtp_rejected");
        failuresHtml.Should().Contain("socket_timeout");
        failuresHtml.Contains("LeaseToken", StringComparison.OrdinalIgnoreCase).Should().BeFalse();
        failuresHtml.Should().NotContain("top-secret-value");

        using var failedDetailRequest = CreateAuthorizedRequest($"/admin/jobs/runs/{permanentRun.Id}");
        using var failedDetailResponse = await client.SendAsync(failedDetailRequest);
        var failedDetailHtml = await failedDetailResponse.Content.ReadAsStringAsync();

        failedDetailResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        failedDetailHtml.Should().Contain("Execution timeline");
        failedDetailHtml.Should().Contain("Attempt 1 claimed");
        failedDetailHtml.Should().Contain("Attempt 1 permanently failed");
        failedDetailHtml.Should().Contain("smtp_rejected");
        failedDetailHtml.Should().Contain("Failure workbench");
        failedDetailHtml.Should().NotContain("top-secret-value");
    }

    private static HttpRequestMessage CreateAuthorizedRequest(string uri)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.Add("X-Test-User", "operator");
        return request;
    }

    private sealed class HeaderAuthenticationHandler : AuthenticationHandler<AuthenticationSchemeOptions>
    {
        public HeaderAuthenticationHandler(
            IOptionsMonitor<AuthenticationSchemeOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder)
            : base(options, logger, encoder)
        {
        }

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            if (!Request.Headers.TryGetValue("X-Test-User", out var value)
                || string.IsNullOrWhiteSpace(value))
            {
                return Task.FromResult(AuthenticateResult.NoResult());
            }

            var identity = new ClaimsIdentity(
                new[] { new Claim(ClaimTypes.Name, value.ToString()) },
                Scheme.Name);
            var principal = new ClaimsPrincipal(identity);
            var ticket = new AuthenticationTicket(principal, Scheme.Name);
            return Task.FromResult(AuthenticateResult.Success(ticket));
        }
    }
}