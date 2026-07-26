using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Tests.ControlPlane;

public sealed class ExecutionAdmissionTests
{
    [Fact]
    public async Task Admission_claims_the_envelope_run_and_reports_terminal_duplicates()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        using var provider = services.BuildServiceProvider();
        var jobs = provider.GetRequiredService<JobControlPlane>();
        var workers = provider.GetRequiredService<WorkerControlPlane>();

        var receipt = await jobs.SubmitAsync(
            new EnqueueJobRequest(
                "order-push-2",
                "{\"orderId\":\"O-1001\"}",
                "orders.push",
                IdempotencyKey: "order-event:1001",
                ConcurrencyKey: "order:O-1001"));
        var session = await workers.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-1",
                "session-1",
                "test",
                "localhost",
                1,
                new[] { "orders.push" },
                new[] { "order-push-2" },
                new Dictionary<string, string>()));

        var admitted = await workers.AdmitAsync(new AdmitExecutionRequest(
            session.WorkerId,
            session.SessionId,
            session.SessionEpoch,
            1,
            receipt.Handle.JobId,
            new[] { "orders.push" },
            new[] { "order-push-2" }));

        admitted.Status.Should().Be(ExecutionAdmissionStatus.Admitted);
        admitted.Job!.RunId.Should().Be(receipt.Handle.JobId);

        var duplicate = await workers.AdmitAsync(new AdmitExecutionRequest(
            session.WorkerId,
            session.SessionId,
            session.SessionEpoch,
            1,
            receipt.Handle.JobId,
            new[] { "orders.push" },
            new[] { "order-push-2" }));

        duplicate.Status.Should().Be(ExecutionAdmissionStatus.Retry);
    }
}
