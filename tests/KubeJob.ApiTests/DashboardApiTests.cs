using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Encodings.Web;
using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
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
    public async Task Dashboard_schedule_page_exposes_schedule_management_when_enabled()
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
            options.AllowMutatingActions = true;
        });

        await using var app = builder.Build();
        app.UseRouting();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapControllers();
        await app.StartAsync();

        var now = DateTimeOffset.UtcNow;
        await app.Services.GetRequiredService<IJobScheduleStore>().UpsertAsync(
            new JobScheduleRecord
            {
                Id = "dashboard-schedule",
                JobKey = "demo.print",
                PayloadJson = "{}",
                CronExpression = "* * * * *",
                TimeZoneId = "UTC",
                Queue = "default",
                MisfirePolicy = MisfirePolicy.FireOnce,
                ConcurrencyPolicy = ScheduleConcurrencyPolicy.SkipIfRunning,
                MaxAttempts = 1,
                TimeoutSeconds = 60,
                Enabled = true,
                NextFireAt = now.AddMinutes(1),
                CreatedAt = now,
                UpdatedAt = now
            },
            CancellationToken.None);
        await app.Services.GetRequiredService<InMemoryJobRuntimeStore>().RegisterAsync(
            new RegisterWorkerSessionRequest(
                "schedule-preview-worker",
                "schedule-preview-session",
                "test",
                "localhost",
                1,
                new[] { "default" },
                new[] { "demo.print" },
                new Dictionary<string, string>()),
            CancellationToken.None);

        using var request = CreateAuthorizedRequest("/admin/jobs/schedules");
        using var response = await app.GetTestClient().SendAsync(request);
        var html = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        html.Should().Contain("New recurring job");
        html.Should().Contain("<dialog");
        html.Should().Contain("data-open=\"false\"");
        html.Should().Contain("Create Schedule");
        html.Should().Contain("name=\"CreateForm.Id\"");
        html.Should().Contain("name=\"CreateForm.PayloadJson\"");
        html.Should().Contain("name=\"CreateForm.CronExpression\"");
        html.Should().Contain("schedule-job-key-suggestions");
        html.Should().Contain("<option value=\"demo.print\"");
        html.Should().Contain("schedule-preview");
        html.Should().Contain("Delete");
        html.Should().Contain("Pause");
        html.Should().Contain("Job key / queue");
        html.Should().Contain("name=\"expectedVersion\"");
        html.Should().Contain("__RequestVerificationToken");

        using var previewRequest = CreateAuthorizedRequest(
            "/admin/jobs/schedules/preview?cronExpression=*/5%20*%20*%20*%20*&timeZoneId=UTC");
        using var previewResponse = await app.GetTestClient().SendAsync(previewRequest);
        var previewJson = await previewResponse.Content.ReadAsStringAsync();

        previewResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        previewJson.Should().Contain("occurrences");
        previewJson.Should().Contain("display");
        previewJson.Should().Contain("timeZoneId");
        previewJson.Should().Contain("+00:00");

        using var invalidPolicyRequest = new HttpRequestMessage(
            HttpMethod.Put,
            "/api/kubejob/schedules/invalid-policy")
        {
            Content = JsonContent.Create(new UpsertCronScheduleRequest(
                "demo.print",
                "{}",
                "*/5 * * * *",
                MisfirePolicy: (MisfirePolicy)99,
                ConcurrencyPolicy: (ScheduleConcurrencyPolicy)99))
        };
        using var invalidPolicyResponse = await app.GetTestClient().SendAsync(invalidPolicyRequest);

        invalidPolicyResponse.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

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
        overviewHtml.Should().Contain("Read-only");
        overviewHtml.Should().Contain("aria-label=\"Dashboard navigation\"");
        overviewHtml.Should().Contain("aria-current=\"page\"");
        overviewHtml.Should().Contain("id=\"dashboard-nav-toggle\"");
        overviewHtml.Should().Contain("aria-expanded=\"false\"");
        overviewHtml.Should().Contain("Job keys");
        overviewHtml.Should().Contain("Ready jobs are waiting, but no worker is ready.");
        overviewHtml.Should().Contain("Check workers");
        overviewHtml.Should().Contain("Needs attention");
        overviewHtml.Should().Contain("Waiting jobs have no available capacity");
        overviewHtml.Should().Contain("Control-plane shortcuts");
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

        var oldWorkerSession = await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-history-view",
                "session-old",
                "build-1",
                "host-1",
                4,
                new[] { "default" },
                new[] { "mail.send" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        await store.CloseAsync(
            oldWorkerSession.WorkerId,
            oldWorkerSession.SessionId,
            oldWorkerSession.Epoch,
            CancellationToken.None);
        await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-history-view",
                "session-current",
                "build-2",
                "host-1",
                4,
                new[] { "default" },
                new[] { "mail.send" },
                new Dictionary<string, string>()),
            CancellationToken.None);

        using var workersRequest = CreateAuthorizedRequest("/admin/jobs/workers");
        using var workersResponse = await client.SendAsync(workersRequest);
        var workersHtml = await workersResponse.Content.ReadAsStringAsync();

        workersResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        workersHtml.Should().Contain("Active worker sessions");
        workersHtml.Should().Contain("session-current");
        workersHtml.Should().NotContain("session-old");
        workersHtml.Should().Contain("Show history (1)");
        workersHtml.Should().Contain("Refresh manually to see the latest heartbeat");
        workersHtml.Should().NotContain("Auto-refreshes every 15 seconds");

        using var workerHistoryRequest = CreateAuthorizedRequest("/admin/jobs/workers?history=true");
        using var workerHistoryResponse = await client.SendAsync(workerHistoryRequest);
        var workerHistoryHtml = await workerHistoryResponse.Content.ReadAsStringAsync();

        workerHistoryResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        workerHistoryHtml.Should().Contain("All worker sessions");
        workerHistoryHtml.Should().Contain("session-current");
        workerHistoryHtml.Should().Contain("session-old");
        workerHistoryHtml.Should().Contain("No active execution slots");

        using var jobTypesRequest = CreateAuthorizedRequest("/admin/jobs/job-types");
        using var jobTypesResponse = await client.SendAsync(jobTypesRequest);
        var jobTypesHtml = await jobTypesResponse.Content.ReadAsStringAsync();

        jobTypesResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        jobTypesHtml.Should().Contain("Job keys");
        jobTypesHtml.Should().Contain("mail.send");
        jobTypesHtml.Should().Contain("Ready");
        jobTypesHtml.Should().Contain("worker-dashboard");
        jobTypesHtml.Should().Contain("View Runs");
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
