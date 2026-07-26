using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

public sealed class WorkerAndDashboardHardeningTests
{
    [Fact]
    public void Worker_options_are_normalized_before_registration()
    {
        var options = new KubeJobWorkerOptions
        {
            ServerEndpoint = "https://jobs.example.test/control",
            WorkerId = " worker-a ",
            BuildId = " build-42 ",
            Queues = new List<string> { " default ", "default", " mail " },
            Labels = new Dictionary<string, string>
            {
                [" env "] = "production"
            }
        };

        options.Validate();

        options.ServerEndpoint.Should().Be("https://jobs.example.test/control/");
        options.WorkerId.Should().Be("worker-a");
        options.BuildId.Should().Be("build-42");
        options.Queues.Should().Equal("default", "mail");
        options.Labels.Should().HaveCount(1);
        options.Labels["env"].Should().Be("production");
    }

    [Fact]
    public void Worker_options_reject_ambiguous_normalized_label_keys()
    {
        var options = new KubeJobWorkerOptions
        {
            Labels = new Dictionary<string, string>
            {
                ["env"] = "production",
                [" env "] = "staging"
            }
        };

        var action = options.Validate;

        action.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*duplicate key 'env'*");
    }

    [Fact]
    public async Task Rejected_heartbeat_fails_the_worker_session_instead_of_polling_forever()
    {
        await using var services = new ServiceCollection().BuildServiceProvider();
        var registry = new JobHandlerRegistry(new[] { new NoopInvoker() });
        var runtime = new RejectingHeartbeatRuntimeClient();
        var options = Options.Create(new KubeJobWorkerOptions
        {
            WorkerId = "worker-fenced",
            Queues = new List<string> { "default" },
            MaxConcurrentJobs = 1,
            ClaimBatchSize = 1,
            EmptyPollDelay = TimeSpan.FromMilliseconds(10),
            HeartbeatInterval = TimeSpan.FromMilliseconds(10),
            LeaseRenewalInterval = TimeSpan.FromMilliseconds(20),
            DrainTimeout = TimeSpan.Zero
        });
        using var claimTrigger = new WorkerClaimTrigger();
        using var worker = new WorkerRuntimeService(
            services.GetRequiredService<IServiceScopeFactory>(),
            registry,
            runtime,
            claimTrigger,
            options,
            NullLogger<WorkerRuntimeService>.Instance);

        await worker.StartAsync(CancellationToken.None);
        var action = async () => await worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(2));

        await action.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*was rejected by the control plane*restart with a new SessionId*");
        runtime.HeartbeatCalls.Should().Be(1);
    }

    [Fact]
    public async Task Dashboard_queries_are_payload_gated_credential_free_and_bounded()
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send",
                "{\"large\":\"payload\"}",
                "mail",
                0,
                DateTimeOffset.UtcNow,
                null,
                null,
                3,
                60),
            CancellationToken.None)).Run;

        for (var index = 0; index < 3; index++)
        {
            await store.RegisterAsync(
                new RegisterWorkerSessionRequest(
                    $"worker-{index}",
                    $"session-{index}",
                    "test",
                    "localhost",
                    1,
                    new[] { "mail" },
                    new[] { "mail.send" },
                    new Dictionary<string, string>()),
                CancellationToken.None);
        }

        var runs = await store.GetRunsAsync(
            new DashboardRunQuery(PageSize: 10),
            CancellationToken.None);
        var hiddenDetails = await store.GetRunDetailsAsync(
            run.Id,
            includePayload: false,
            CancellationToken.None);
        var visibleDetails = await store.GetRunDetailsAsync(
            run.Id,
            includePayload: true,
            CancellationToken.None);
        var sessions = await store.GetWorkerSessionsAsync(2, CancellationToken.None);

        runs.Items.Should().ContainSingle();
        typeof(DashboardRunSummary).GetProperty("PayloadJson").Should().BeNull();
        hiddenDetails.Should().NotBeNull();
        hiddenDetails!.PayloadJson.Should().BeNull();
        visibleDetails!.PayloadJson.Should().Be("{\"large\":\"payload\"}");
        typeof(DashboardAttemptSummary).GetProperty("LeaseToken").Should().BeNull();
        sessions.Should().HaveCount(2);
    }

    [Fact]
    public async Task Dashboard_can_filter_runs_by_exact_job_key()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(
            new SubmitJobCommand("mail.send", "{}", "default", 0, DateTimeOffset.UtcNow, null, null, 1, 60),
            CancellationToken.None);
        await store.SubmitAsync(
            new SubmitJobCommand("mail.send.v2", "{}", "default", 0, DateTimeOffset.UtcNow, null, null, 1, 60),
            CancellationToken.None);

        var result = await store.GetRunsAsync(
            new DashboardRunQuery(PageSize: 10, JobKey: "mail.send", ExactJobKey: true),
            CancellationToken.None);

        result.Items.Should().ContainSingle();
        result.Items[0].JobKey.Should().Be("mail.send");
    }

    [Fact]
    public async Task Dashboard_overview_reports_oldest_ready_run_and_recent_activity()
    {
        var store = new InMemoryJobRuntimeStore();
        var completedAvailableAt = DateTimeOffset.UtcNow.AddMinutes(-15);
        await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send",
                "{}",
                "default",
                0,
                completedAvailableAt,
                null,
                null,
                1,
                60),
            CancellationToken.None);
        var session = await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-dashboard",
                "session-dashboard",
                "test",
                "localhost",
                1,
                new[] { "default" },
                new[] { "mail.send" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        var claim = (await store.ClaimAsync(
            new ClaimJobsRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                1,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        var completion = await store.CompleteAsync(
            new CompleteAttemptRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                claim.RunId,
                claim.AttemptId,
                claim.AttemptNumber,
                claim.LeaseToken,
                JobAttemptOutcome.Succeeded),
            TimeSpan.Zero,
            CancellationToken.None);

        var oldestReadyAt = DateTimeOffset.UtcNow.AddMinutes(-8);
        await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send",
                "{}",
                "slow",
                0,
                oldestReadyAt,
                null,
                null,
                1,
                60),
            CancellationToken.None);
        await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send",
                "{}",
                "slow",
                0,
                DateTimeOffset.UtcNow.AddMinutes(10),
                null,
                null,
                1,
                60),
            CancellationToken.None);

        var overview = await store.GetOverviewAsync(10, CancellationToken.None);
        var slowQueue = overview.Queues.Single(queue => queue.Queue == "slow");

        completion.Accepted.Should().BeTrue();
        overview.LastHour.SucceededRuns.Should().Be(1);
        overview.LastHour.UnsuccessfulRuns.Should().Be(0);
        slowQueue.PendingRuns.Should().Be(2);
        slowQueue.RunningRuns.Should().Be(0);
        slowQueue.OldestReadyAt.Should().BeCloseTo(oldestReadyAt, TimeSpan.FromMilliseconds(1));
        overview.ObservedAt.Should().BeAfter(oldestReadyAt);
    }

    private sealed class NoopInvoker : IJobHandlerInvoker
    {
        public string JobKey => "test.job";

        public Type PayloadType => typeof(object);

        public ValueTask InvokeAsync(
            IServiceProvider serviceProvider,
            string payloadJson,
            JobExecutionContext context,
            CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class RejectingHeartbeatRuntimeClient : IWorkerRuntimeClient
    {
        public int HeartbeatCalls { get; private set; }

        public ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
            RegisterWorkerSessionRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(
            new RegisterWorkerSessionResponse(
                request.WorkerId,
                request.SessionId,
                1,
                DateTimeOffset.UtcNow));

        public ValueTask<bool> HeartbeatAsync(
            WorkerHeartbeatRequest request,
            CancellationToken cancellationToken)
        {
            HeartbeatCalls++;
            return ValueTask.FromResult(false);
        }

        public ValueTask<bool> CloseAsync(
            WorkerHeartbeatRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(true);

        public ValueTask<ClaimJobsResponse> ClaimAsync(
            ClaimJobsRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(
            new ClaimJobsResponse(Array.Empty<ClaimedJob>()));

        public ValueTask<AdmitExecutionResponse> AdmitAsync(
            AdmitExecutionRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(
            new AdmitExecutionResponse(ExecutionAdmissionStatus.Retry));

        public ValueTask<RenewLeasesResponse> RenewLeasesAsync(
            RenewLeasesRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(
            new RenewLeasesResponse(Array.Empty<LeaseRenewalResult>()));

        public ValueTask<CompleteAttemptResponse> CompleteAsync(
            CompleteAttemptRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(
            new CompleteAttemptResponse(true, JobPhase.Succeeded, false));
    }
}
