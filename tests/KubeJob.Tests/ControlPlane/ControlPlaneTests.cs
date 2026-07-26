using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Tests.ControlPlane;

public sealed class ControlPlaneTests
{
    [Fact]
    public async Task Job_submission_has_one_validation_and_idempotency_path()
    {
        using var provider = CreateProvider();
        var controlPlane = provider.GetRequiredService<JobControlPlane>();
        var request = new EnqueueJobRequest(
            "sample.data",
            "{\"value\":42}",
            IdempotencyKey: "message:42");

        var first = await controlPlane.SubmitAsync(request);
        var replay = await controlPlane.SubmitAsync(request);
        var invalid = async () => await controlPlane.SubmitAsync(
            request with { PayloadJson = "not-json" });

        first.Existing.Should().BeFalse();
        replay.Existing.Should().BeTrue();
        replay.Handle.Should().Be(first.Handle);
        var exception = await invalid.Should().ThrowAsync<ControlPlaneValidationException>();
        exception.Which.Code.Should().Be("invalid_job_payload");
    }

    [Fact]
    public async Task Message_ingress_uses_source_and_message_id_for_redelivery_idempotency()
    {
        using var provider = CreateProvider();
        var ingress = provider.GetRequiredService<IJobMessageIngress>();
        var message = new JobIngressMessage(
            "rabbitmq.orders",
            "delivery-42",
            new EnqueueJobRequest("sample.data", "{\"value\":42}"));

        var first = await ingress.SubmitAsync(message);
        var replay = await ingress.SubmitAsync(message);
        var conflicting = async () => await ingress.SubmitAsync(
            message with
            {
                Job = message.Job with { PayloadJson = "{\"value\":43}" }
            });

        first.Existing.Should().BeFalse();
        replay.Existing.Should().BeTrue();
        replay.JobId.Should().Be(first.JobId);
        await conflicting.Should().ThrowAsync<IdempotencyConflictException>();
    }

    [Fact]
    public async Task Message_ingress_rejects_missing_broker_identity()
    {
        using var provider = CreateProvider();
        var ingress = provider.GetRequiredService<IJobMessageIngress>();
        var invalid = async () => await ingress.SubmitAsync(
            new JobIngressMessage(
                "rabbitmq.orders",
                " ",
                new EnqueueJobRequest("sample.data", "{}")));

        var exception = await invalid.Should().ThrowAsync<ControlPlaneValidationException>();
        exception.Which.Code.Should().Be("invalid_ingress_identity");
    }

    [Fact]
    public async Task Worker_claim_policy_is_applied_before_both_transport_adapters()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.Configure<JobRuntimeOptions>(options => options.MaxClaimBatchSize = 1);
        using var provider = services.BuildServiceProvider();
        var jobs = provider.GetRequiredService<JobControlPlane>();
        var workers = provider.GetRequiredService<WorkerControlPlane>();

        await jobs.SubmitAsync(new EnqueueJobRequest("sample.data", "{\"value\":1}"));
        await jobs.SubmitAsync(new EnqueueJobRequest("sample.data", "{\"value\":2}"));
        var registration = await workers.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-1",
                "session-1",
                "test",
                "localhost",
                2,
                new[] { "default" },
                new[] { "sample.data" },
                new Dictionary<string, string>()));

        var claim = await workers.ClaimAsync(
            new ClaimJobsRequest(
                registration.WorkerId,
                registration.SessionId,
                registration.SessionEpoch,
                2,
                new[] { "default" },
                new[] { "sample.data" }));

        claim.Jobs.Should().ContainSingle();
    }

    [Fact]
    public async Task Worker_registration_validation_is_transport_independent()
    {
        using var provider = CreateProvider();
        var controlPlane = provider.GetRequiredService<WorkerControlPlane>();
        var invalid = async () => await controlPlane.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-1",
                "session-1",
                "test",
                "localhost",
                1,
                Array.Empty<string>(),
                new[] { "sample.data" },
                new Dictionary<string, string>()));

        var exception = await invalid.Should().ThrowAsync<ControlPlaneValidationException>();
        exception.Which.Code.Should().Be("invalid_worker_registration");
    }

    [Fact]
    public async Task Schedule_validation_and_next_occurrence_are_owned_by_control_plane()
    {
        using var provider = CreateProvider();
        var controlPlane = provider.GetRequiredService<ScheduleControlPlane>();
        var invalid = async () => await controlPlane.UpsertCronAsync(
            "sample-every-minute",
            new UpsertCronScheduleRequest(
                "sample.data",
                "{}",
                "not-a-cron"));

        var exception = await invalid.Should().ThrowAsync<ControlPlaneValidationException>();
        exception.Which.Code.Should().Be("invalid_schedule");

        var preview = controlPlane.PreviewCron(
            "* * * * *",
            "UTC",
            DateTimeOffset.UtcNow,
            3);
        var schedule = await controlPlane.CreateCronAsync(
            "sample-every-minute",
            new UpsertCronScheduleRequest(
                "sample.data",
                "{}",
                "* * * * *"));
        var duplicate = await controlPlane.CreateCronAsync(
            "sample-every-minute",
            new UpsertCronScheduleRequest(
                "sample.data",
                "{}",
                "* * * * *"));

        preview.TimeZoneId.Should().Be("UTC");
        preview.Occurrences.Should().HaveCount(3).And.BeInAscendingOrder();
        schedule!.ScheduleId.Should().Be("sample-every-minute");
        schedule.NextFireAt.Should().BeAfter(DateTimeOffset.UtcNow);
        duplicate.Should().BeNull();
    }

    private static ServiceProvider CreateProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        return services.BuildServiceProvider();
    }
}
