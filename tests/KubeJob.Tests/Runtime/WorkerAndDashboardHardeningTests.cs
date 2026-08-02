using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
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
            Queues = new List<string> { "test.queue" },
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
    public async Task Rejected_heartbeat_fails_the_hosted_service_for_supervisor_restart()
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

        // The control plane rejects the session identity: the worker must NOT
        // restart internally with a new SessionId (that would spin against the
        // same rejection); it fails the hosted service so the supervisor
        // restarts the process.
        var completed = await Task.WhenAny(
            worker.ExecuteTask!,
            Task.Delay(TimeSpan.FromSeconds(5)));
        completed.Should().BeSameAs(worker.ExecuteTask);
        runtime.HeartbeatCalls.Should().BeGreaterThanOrEqualTo(1);
        runtime.RegisterCalls.Should().Be(1);
        worker.ExecuteTask!.IsFaulted.Should().BeTrue();
        worker.ExecuteTask!.Exception!.InnerException!
            .Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("fenced");

        // StopAsync returns promptly even after the hosted service failed.
        await worker.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Fenced_session_with_uncooperative_handler_still_fails_the_hosted_service()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.UseInProcessKubeJobWorkerTransport();
        await using var provider = services.BuildServiceProvider();

        var jobs = provider.GetRequiredService<JobControlPlane>();
        var inner = provider.GetRequiredService<IWorkerRuntimeClient>();
        var run = await jobs.SubmitAsync(
            new EnqueueJobRequest(
                "test.echo",
                "{}",
                "default",
                0,
                DateTimeOffset.UtcNow,
                null,
                null,
                1,
                300),
            CancellationToken.None);

        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = Options.Create(new KubeJobWorkerOptions
        {
            WorkerId = "worker-uncooperative",
            Queues = new List<string> { "default" },
            MaxConcurrentJobs = 1,
            ClaimBatchSize = 1,
            EmptyPollDelay = TimeSpan.FromHours(1),
            HeartbeatInterval = TimeSpan.FromMilliseconds(10),
            LeaseRenewalInterval = TimeSpan.FromMilliseconds(10),
            DrainTimeout = TimeSpan.FromMilliseconds(100)
        });
        using var worker = new WorkerRuntimeService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            new JobHandlerRegistry(new[] { new UncooperativeInvoker("test.echo", parked) }),
            new RejectingHeartbeatDelegatingClient(inner),
            new WorkerClaimTrigger(),
            options,
            NullLogger<WorkerRuntimeService>.Instance);

        await worker.StartAsync(CancellationToken.None);

        // A handler that ignores cancellation keeps the session from settling;
        // the fence deadline must still fail the hosted service.
        var completed = await Task.WhenAny(
            worker.ExecuteTask!,
            Task.Delay(TimeSpan.FromSeconds(5)));
        completed.Should().BeSameAs(worker.ExecuteTask);
        worker.ExecuteTask!.IsFaulted.Should().BeTrue();
        worker.ExecuteTask!.Exception!.InnerException!
            .Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("fenced");

        // Release the parked handler so the abandoned session work settles.
        parked.SetResult();
        await worker.StopAsync(CancellationToken.None);
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
            new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1)),
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
        public int RegisterCalls { get; private set; }
        public int HeartbeatCalls { get; private set; }

        public ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
            RegisterWorkerSessionRequest request,
            CancellationToken cancellationToken)
        {
            RegisterCalls++;
            return ValueTask.FromResult(new RegisterWorkerSessionResponse(
                request.WorkerId,
                request.SessionId,
                RegisterCalls,
                DateTimeOffset.UtcNow));
        }

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

        public ValueTask<AdmitExecutionBatchResponse> AdmitBatchAsync(
            AdmitExecutionBatchRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(
            new AdmitExecutionBatchResponse(
                request.RunIds.Select(runId => new AdmitExecutionResult(
                    runId,
                    ExecutionAdmissionStatus.Retry)).ToArray()));

        public ValueTask<RenewLeasesResponse> RenewLeasesAsync(
            RenewLeasesRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(
            new RenewLeasesResponse(Array.Empty<LeaseRenewalResult>()));

        public ValueTask<CompleteAttemptResponse> CompleteAsync(
            CompleteAttemptRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(
            new CompleteAttemptResponse(true, JobPhase.Succeeded, false));

        public ValueTask<bool> RequeueExecutionAsync(
            RequeueExecutionRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }

    /// <summary>
    /// Delegates every call to a real in-process client but rejects heartbeats,
    /// so a genuinely claimed attempt can run while the session gets fenced.
    /// </summary>
    private sealed class RejectingHeartbeatDelegatingClient : IWorkerRuntimeClient
    {
        private readonly IWorkerRuntimeClient _inner;

        public RejectingHeartbeatDelegatingClient(IWorkerRuntimeClient inner)
        {
            _inner = inner;
        }

        public ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
            RegisterWorkerSessionRequest request,
            CancellationToken cancellationToken) =>
            _inner.RegisterAsync(request, cancellationToken);

        public ValueTask<bool> HeartbeatAsync(
            WorkerHeartbeatRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask<bool> CloseAsync(
            WorkerHeartbeatRequest request,
            CancellationToken cancellationToken) =>
            _inner.CloseAsync(request, cancellationToken);

        public ValueTask<ClaimJobsResponse> ClaimAsync(
            ClaimJobsRequest request,
            CancellationToken cancellationToken) =>
            _inner.ClaimAsync(request, cancellationToken);

        public ValueTask<AdmitExecutionResponse> AdmitAsync(
            AdmitExecutionRequest request,
            CancellationToken cancellationToken) =>
            _inner.AdmitAsync(request, cancellationToken);

        public ValueTask<AdmitExecutionBatchResponse> AdmitBatchAsync(
            AdmitExecutionBatchRequest request,
            CancellationToken cancellationToken) =>
            _inner.AdmitBatchAsync(request, cancellationToken);

        public ValueTask<RenewLeasesResponse> RenewLeasesAsync(
            RenewLeasesRequest request,
            CancellationToken cancellationToken) =>
            _inner.RenewLeasesAsync(request, cancellationToken);

        public ValueTask<CompleteAttemptResponse> CompleteAsync(
            CompleteAttemptRequest request,
            CancellationToken cancellationToken) =>
            _inner.CompleteAsync(request, cancellationToken);

        public ValueTask<bool> RequeueExecutionAsync(
            RequeueExecutionRequest request,
            CancellationToken cancellationToken) =>
            _inner.RequeueExecutionAsync(request, cancellationToken);
    }

    private sealed class UncooperativeInvoker : IJobHandlerInvoker
    {
        private readonly TaskCompletionSource _release;

        public UncooperativeInvoker(string jobKey, TaskCompletionSource release)
        {
            JobKey = jobKey;
            _release = release;
        }

        public string JobKey { get; }

        public Type PayloadType => typeof(object);

        // Deliberately ignores the cancellation token: this is the
        // uncooperative-handler scenario the fence deadline must survive.
        public async ValueTask InvokeAsync(
            IServiceProvider serviceProvider,
            string payloadJson,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            await _release.Task;
        }
    }
}
