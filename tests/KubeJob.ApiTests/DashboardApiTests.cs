using System.Net;
using System.Security.Claims;
using System.Text.Encodings.Web;
using FluentAssertions;
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
        var run = (await store.SubmitAsync(
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
        overviewHtml.Should().Contain("Runs & Attempts");
        overviewHtml.Contains("LeaseToken", StringComparison.OrdinalIgnoreCase).Should().BeFalse();

        using var detailRequest = CreateAuthorizedRequest($"/admin/jobs/runs/{run.Id}");
        using var detailResponse = await client.SendAsync(detailRequest);
        var detailHtml = await detailResponse.Content.ReadAsStringAsync();

        detailResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        detailHtml.Should().Contain("Payload display is disabled");
        detailHtml.Should().NotContain("top-secret-value");
        detailHtml.Should().NotContain(">Cancel<");
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
