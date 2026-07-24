using System.Net;
using System.Security.Claims;
using System.Text.Encodings.Web;
using FluentAssertions;
using KubeJob.Server.Extensions;
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
    public async Task Dashboard_uses_custom_route_and_scoped_authorization_policy()
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

        var client = app.GetTestClient();

        using var anonymousResponse = await client.GetAsync("/admin/jobs");
        anonymousResponse.StatusCode.Should().Be(HttpStatusCode.Unauthorized);

        using var request = new HttpRequestMessage(HttpMethod.Get, "/admin/jobs");
        request.Headers.Add("X-Test-User", "operator");
        using var authorizedResponse = await client.SendAsync(request);
        var html = await authorizedResponse.Content.ReadAsStringAsync();

        authorizedResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        html.Should().Contain("Runtime Overview");
        html.Should().Contain("Runs &amp; Attempts");
        html.Should().NotContain("LeaseToken", StringComparison.OrdinalIgnoreCase);
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
