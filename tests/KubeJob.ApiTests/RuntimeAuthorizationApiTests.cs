using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Encodings.Web;
using FluentAssertions;
using KubeJob.Core.Runtime;
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

public sealed class RuntimeAuthorizationApiTests
{
    [Fact]
    public async Task Client_and_worker_endpoints_support_independent_policies()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services
            .AddAuthentication("kubejob-test")
            .AddScheme<AuthenticationSchemeOptions, ScopeHeaderAuthenticationHandler>(
                "kubejob-test",
                _ => { });
        builder.Services.AddAuthorization(options =>
        {
            options.AddPolicy("KubeJobClient", policy =>
            {
                policy.AddAuthenticationSchemes("kubejob-test");
                policy.RequireClaim("scope", "client");
            });
            options.AddPolicy("KubeJobWorker", policy =>
            {
                policy.AddAuthenticationSchemes("kubejob-test");
                policy.RequireClaim("scope", "worker");
            });
        });
        builder.Services.AddKubeJobServer(options =>
        {
            options.ClientAuthorizationPolicy = "KubeJobClient";
            options.WorkerAuthorizationPolicy = "KubeJobWorker";
        });

        await using var app = builder.Build();
        app.UseRouting();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapControllers();
        await app.StartAsync();

        var client = app.GetTestClient();
        using var anonymousJobs = await client.GetAsync("/api/kubejob/jobs/missing");
        using var anonymousWorker = await client.PostAsJsonAsync(
            "/api/kubejob/runtime/workers/register",
            WorkerRegistration());
        anonymousJobs.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        anonymousWorker.StatusCode.Should().Be(HttpStatusCode.Unauthorized);

        using var clientJobRequest = Authorized(
            HttpMethod.Post,
            "/api/kubejob/jobs",
            "client",
            new EnqueueJobRequest("mail.send", "{}", MaxAttempts: 1, TimeoutSeconds: 60));
        using var clientJobResponse = await client.SendAsync(clientJobRequest);
        clientJobResponse.StatusCode.Should().Be(HttpStatusCode.Accepted);

        using var clientWorkerRequest = Authorized(
            HttpMethod.Post,
            "/api/kubejob/runtime/workers/register",
            "client",
            WorkerRegistration());
        using var clientWorkerResponse = await client.SendAsync(clientWorkerRequest);
        clientWorkerResponse.StatusCode.Should().Be(HttpStatusCode.Forbidden);

        using var workerRequest = Authorized(
            HttpMethod.Post,
            "/api/kubejob/runtime/workers/register",
            "worker",
            WorkerRegistration());
        using var workerResponse = await client.SendAsync(workerRequest);
        workerResponse.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    private static RegisterWorkerSessionRequest WorkerRegistration() =>
        new(
            "authorized-worker",
            Guid.NewGuid().ToString("N"),
            "test",
            "localhost",
            1,
            new[] { "default" },
            new[] { "mail.send" },
            new Dictionary<string, string>());

    private static HttpRequestMessage Authorized<TBody>(
        HttpMethod method,
        string uri,
        string scope,
        TBody body)
    {
        var request = new HttpRequestMessage(method, uri)
        {
            Content = JsonContent.Create(body)
        };
        request.Headers.Add("X-Test-Scope", scope);
        return request;
    }

    private sealed class ScopeHeaderAuthenticationHandler :
        AuthenticationHandler<AuthenticationSchemeOptions>
    {
        public ScopeHeaderAuthenticationHandler(
            IOptionsMonitor<AuthenticationSchemeOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder)
            : base(options, logger, encoder)
        {
        }

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            if (!Request.Headers.TryGetValue("X-Test-Scope", out var scope)
                || string.IsNullOrWhiteSpace(scope))
            {
                return Task.FromResult(AuthenticateResult.NoResult());
            }

            var identity = new ClaimsIdentity(
                new[]
                {
                    new Claim(ClaimTypes.Name, "kubejob-test"),
                    new Claim("scope", scope.ToString())
                },
                Scheme.Name);
            var ticket = new AuthenticationTicket(
                new ClaimsPrincipal(identity),
                Scheme.Name);
            return Task.FromResult(AuthenticateResult.Success(ticket));
        }
    }
}
