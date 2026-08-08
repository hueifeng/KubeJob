using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

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
    public async Task Job_submission_persists_the_canonical_managed_queue()
    {
        var options = new QueueDeliveryOptions();
        var store = new InMemoryJobRuntimeStore();
        var controlPlane = new JobControlPlane(
            store,
            store,
            new ConfigurationQueueRouter(Options.Create(options)),
            Options.Create(new JobRuntimeOptions()),
            new OutboxPublisherSignal());

        var receipt = await controlPlane.SubmitAsync(
            new EnqueueJobRequest("sample.data", "{}", Queue: " orders.push "));

        var run = await store.GetRunAsync(receipt.Handle.JobId, CancellationToken.None);
        run!.Queue.Should().Be("orders.push");
        run.DeliveryProfile.Should().Be(ExecutionDeliveryProfile.Pull);
        run.TransportId.Should().BeNull();
    }

    [Fact]
    public async Task Managed_cancel_is_database_authoritative_and_does_not_emit_broker_cancel_message()
    {
        var store = new InMemoryJobRuntimeStore();
        var controlPlane = new JobControlPlane(
            store,
            store,
            new ConfigurationQueueRouter(Options.Create(new QueueDeliveryOptions())),
            Options.Create(new JobRuntimeOptions()),
            new OutboxPublisherSignal());

        var receipt = await controlPlane.SubmitAsync(
            new EnqueueJobRequest("sample.data", "{}"));

        (await controlPlane.RequestCancelAsync(receipt.Handle.JobId, "cancel managed run"))
            .Should().BeTrue();

        var run = await store.GetRunAsync(receipt.Handle.JobId, CancellationToken.None);
        run!.CancelRequested.Should().BeTrue();
        run.Phase.Should().Be(JobPhase.Canceled);

        var outbox = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromMinutes(1),
            10,
            CancellationToken.None);
        outbox.Should().OnlyContain(message => message.EventType == OutboxEventTypes.WorkAvailable);
    }

    [Fact]
    public async Task Job_submission_rejects_invalid_retry_and_terminal_action_configuration()
    {
        using var provider = CreateProvider();
        var controlPlane = provider.GetRequiredService<JobControlPlane>();

        var invalidRetry = async () => await controlPlane.SubmitAsync(
            new EnqueueJobRequest(
                "sample.data",
                "{}",
                RetryPolicy: new RetryPolicy(
                    BackoffStrategy.Fixed,
                    TimeSpan.Zero,
                    TimeSpan.Zero)));
        var retryException = await invalidRetry.Should().ThrowAsync<ControlPlaneValidationException>();
        retryException.Which.Code.Should().Be("invalid_job_retry_policy");

        var invalidAction = async () => await controlPlane.SubmitAsync(
            new EnqueueJobRequest(
                "sample.data",
                "{}",
                Continuation: new Continuation
                {
                    JobKey = "sample.followup",
                    PayloadJson = "not-json",
                    Trigger = ContinuationTrigger.OnSuccess
                }));
        var actionException = await invalidAction.Should().ThrowAsync<ControlPlaneValidationException>();
        actionException.Which.Code.Should().Be("invalid_job_terminal_action");
    }

    [Fact]
    public async Task Job_submission_rejects_cross_queue_terminal_actions_until_their_route_is_resolved()
    {
        using var provider = CreateProvider();
        var controlPlane = provider.GetRequiredService<JobControlPlane>();

        var invalid = async () => await controlPlane.SubmitAsync(
            new EnqueueJobRequest(
                "sample.data",
                "{}",
                Queue: "orders.push",
                Continuation: new Continuation
                {
                    JobKey = "sample.followup",
                    Queue = "billing.push"
                }));

        var exception = await invalid.Should().ThrowAsync<ControlPlaneValidationException>();
        exception.Which.Code.Should().Be("cross_queue_terminal_action_not_supported");
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
    public async Task Job_submission_rejects_values_that_exceed_storage_limits()
    {
        using var provider = CreateProvider();
        var controlPlane = provider.GetRequiredService<JobControlPlane>();

        var invalid = async () => await controlPlane.SubmitAsync(
            new EnqueueJobRequest(
                new string('j', 301),
                "{}",
                "default"));

        var exception = await invalid.Should().ThrowAsync<ControlPlaneValidationException>();
        exception.Which.Code.Should().Be("job_submission_field_too_long");
    }

    [Fact]
    public async Task Job_submission_rejects_payloads_that_exceed_the_utf8_limit()
    {
        using var provider = CreateProvider();
        var controlPlane = provider.GetRequiredService<JobControlPlane>();
        var payload = $"{{\"value\":\"{new string('x', 1_048_576)}\"}}";

        var invalid = async () => await controlPlane.SubmitAsync(
            new EnqueueJobRequest("sample.data", payload));

        var exception = await invalid.Should().ThrowAsync<ControlPlaneValidationException>();
        exception.Which.Code.Should().Be("job_payload_too_large");
    }

    [Fact]
    public async Task Job_submission_batch_is_bounded_before_store_mutation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.Configure<JobRuntimeOptions>(options => options.MaxSubmissionBatchSize = 1);
        using var provider = services.BuildServiceProvider();
        var controlPlane = provider.GetRequiredService<JobControlPlane>();

        var invalid = async () => await controlPlane.SubmitBatchAsync(new[]
        {
            new EnqueueJobRequest("sample.data", "{}"),
            new EnqueueJobRequest("sample.data", "{}")
        });

        var exception = await invalid.Should().ThrowAsync<ControlPlaneValidationException>();
        exception.Which.Code.Should().Be("job_submission_batch_too_large");
    }

    [Fact]
    public async Task Managed_worker_claim_policy_is_applied_before_claiming()
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
