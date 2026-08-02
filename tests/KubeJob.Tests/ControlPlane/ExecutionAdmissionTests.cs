using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Tests.ControlPlane;

public sealed class ExecutionAdmissionTests
{
    [Fact]
    public async Task KeyOrdered_queue_does_not_admit_a_later_key_run_while_its_predecessor_is_pending()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer()
            .ConfigureKubeJobQueueRouting(options =>
                options.Queues["orders.push"] = new QueueDefinition
                {
                    OrderingMode = ExecutionOrderingMode.KeyOrdered
                });
        using var provider = services.BuildServiceProvider();
        var jobs = provider.GetRequiredService<JobControlPlane>();
        var workers = provider.GetRequiredService<WorkerControlPlane>();

        var first = await jobs.SubmitAsync(new EnqueueJobRequest(
            "order-push-2", "{\"version\":1}", "orders.push", ConcurrencyKey: "order:1001"));
        var second = await jobs.SubmitAsync(new EnqueueJobRequest(
            "order-push-2", "{\"version\":2}", "orders.push", ConcurrencyKey: "order:1001"));
        var session = await workers.RegisterAsync(new RegisterWorkerSessionRequest(
            "worker-1", "session-1", "test", "localhost", 2,
            new[] { "orders.push" }, new[] { "order-push-2" }, new Dictionary<string, string>()));

        var later = await workers.AdmitAsync(new AdmitExecutionRequest(
            session.WorkerId, session.SessionId, session.SessionEpoch, 2, second.Handle.JobId,
            new[] { "orders.push" }, new[] { "order-push-2" }));
        later.Status.Should().Be(ExecutionAdmissionStatus.Retry);
        later.Reason.Should().Be("run_not_claimable");

        var head = await workers.AdmitAsync(new AdmitExecutionRequest(
            session.WorkerId, session.SessionId, session.SessionEpoch, 2, first.Handle.JobId,
            new[] { "orders.push" }, new[] { "order-push-2" }));
        head.Status.Should().Be(ExecutionAdmissionStatus.Admitted);
    }

    [Fact]
    public async Task KeyOrdered_queue_requires_a_partition_key()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer()
            .ConfigureKubeJobQueueRouting(options =>
                options.Queues["orders.push"] = new QueueDefinition
                {
                    OrderingMode = ExecutionOrderingMode.KeyOrdered
                });
        using var provider = services.BuildServiceProvider();
        var jobs = provider.GetRequiredService<JobControlPlane>();

        var action = () => jobs.SubmitAsync(new EnqueueJobRequest(
            "order-push-2", "{}", "orders.push")).AsTask();

        await action.Should().ThrowAsync<ControlPlaneValidationException>()
            .WithMessage("*ConcurrencyKey*");
    }

    [Fact]
    public async Task Admission_claims_the_envelope_run_and_retries_duplicates_until_terminal()
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
        duplicate.Reason.Should().Be("run_already_running");
    }
}
