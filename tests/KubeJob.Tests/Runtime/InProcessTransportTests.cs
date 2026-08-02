using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Tests.Runtime;

public sealed class InProcessTransportTests
{
    [Fact]
    public void Unified_registration_resolves_in_process_worker_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.UseInProcessKubeJobWorkerTransport();
        services.AddKubeJobWorker(options =>
        {
            options.WorkerId = "unified-worker";
            options.MaxConcurrentJobs = 1;
        });

        using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IWorkerRuntimeClient>()
            .Should().BeOfType<InProcessWorkerRuntimeClient>();
    }

    [Fact]
    public async Task In_process_transport_executes_registration_and_claim_protocol()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.UseInProcessKubeJobWorkerTransport();
        using var provider = services.BuildServiceProvider();

        var submissions = provider.GetRequiredService<IJobSubmissionStore>();
        await submissions.SubmitAsync(
            new SubmitJobCommand("test.echo", "{}", "default", 0, DateTimeOffset.UtcNow, null, null, 1, 30),
            CancellationToken.None);

        var client = provider.GetRequiredService<IWorkerRuntimeClient>();
        var registration = await client.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker", "session", "test", "localhost", 1,
                new[] { "default" },
                new[] { "test.echo" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        var claims = await client.ClaimAsync(
            new ClaimJobsRequest(
                "worker", "session", registration.SessionEpoch, 1,
                new[] { "default" },
                new[] { "test.echo" }),
            CancellationToken.None);

        claims.Jobs.Should().ContainSingle();
    }
}
