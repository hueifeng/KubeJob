using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Covers the batch admission path added for broker-consumer throughput:
/// per-run classification in one claim transaction, worker-side batch envelope
/// processing, and the schedule-to-Run ExecutionLane propagation.
/// </summary>
public sealed class BatchAdmissionTests
{
    [Fact]
    public async Task AdmitBatch_classifies_each_run_and_preserves_input_order()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<InMemoryJobRuntimeStore>();
        var jobs = provider.GetRequiredService<JobControlPlane>();
        var workers = provider.GetRequiredService<WorkerControlPlane>();

        var registration = await workers.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-a", "session-a", null, "localhost", 10,
                new[] { "default" },
                new[] { "test.echo" },
                new Dictionary<string, string>(),
                "default", "default"),
            CancellationToken.None);

        // A claimable pending run.
        var pendingReceipt = await SubmitAsync(jobs, "run-pending");
        // A terminal run: claim then complete as succeeded.
        var terminalReceipt = await SubmitAsync(jobs, "run-terminal");
        await CompleteAsSucceededAsync(workers, terminalReceipt.Handle.JobId, registration);
        // A canceled run.
        var canceledReceipt = await SubmitAsync(jobs, "run-canceled");
        await jobs.RequestCancelAsync(canceledReceipt.Handle.JobId, null, CancellationToken.None);
        // A run already running on another session.
        var runningReceipt = await SubmitAsync(jobs, "run-running");
        await ClaimAsync(workers, runningReceipt.Handle.JobId, registration);
        // A run whose queue the worker does not serve.
        var wrongQueueReceipt = await jobs.SubmitAsync(
            new EnqueueJobRequest("test.echo", "{}", "mail", 0, DateTimeOffset.UtcNow, null, null, 1, 300),
            CancellationToken.None);

        var runIds = new[]
        {
            pendingReceipt.Handle.JobId,
            terminalReceipt.Handle.JobId,
            canceledReceipt.Handle.JobId,
            runningReceipt.Handle.JobId,
            wrongQueueReceipt.Handle.JobId,
            "missing-run"
        };

        var response = await workers.AdmitBatchAsync(
            new AdmitExecutionBatchRequest(
                "worker-a",
                "session-a",
                registration.SessionEpoch,
                10,
                runIds,
                new[] { "default" },
                new[] { "test.echo" },
                "default", "default"),
            CancellationToken.None);

        response.Results.Should().HaveCount(runIds.Length);
        response.Results.Select(result => result.RunId).Should().Equal(runIds);

        var byRunId = response.Results.ToDictionary(result => result.RunId);
        byRunId[pendingReceipt.Handle.JobId].Status.Should().Be(ExecutionAdmissionStatus.Admitted);
        byRunId[pendingReceipt.Handle.JobId].Job.Should().NotBeNull();
        byRunId[terminalReceipt.Handle.JobId].Status.Should().Be(ExecutionAdmissionStatus.AlreadyTerminal);
        byRunId[canceledReceipt.Handle.JobId].Status.Should().Be(ExecutionAdmissionStatus.AlreadyTerminal);
        byRunId[runningReceipt.Handle.JobId].Status.Should().Be(ExecutionAdmissionStatus.Retry);
        byRunId[runningReceipt.Handle.JobId].Reason.Should().Be("run_already_running");
        byRunId[wrongQueueReceipt.Handle.JobId].Status.Should().Be(ExecutionAdmissionStatus.Retry);
        byRunId[wrongQueueReceipt.Handle.JobId].Reason.Should().Be("worker_not_capable");
        byRunId["missing-run"].Status.Should().Be(ExecutionAdmissionStatus.Retry);
        byRunId["missing-run"].Reason.Should().Be("run_not_found");

        // The admitted run is now Running with an attempt owned by this worker.
        var admitted = await store.GetRunAsync(pendingReceipt.Handle.JobId, CancellationToken.None);
        admitted.Should().NotBeNull();
        admitted!.Phase.Should().Be(JobPhase.Running);
        admitted.CurrentSessionId.Should().Be("session-a");
    }

    [Fact]
    public async Task AdmitBatch_rejects_blank_run_ids_and_respects_capacity()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        using var provider = services.BuildServiceProvider();

        var workers = provider.GetRequiredService<WorkerControlPlane>();
        var registration = await workers.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-b", "session-b", null, "localhost", 2,
                new[] { "default" },
                new[] { "test.echo" },
                new Dictionary<string, string>()),
            CancellationToken.None);

        var blank = await workers.AdmitBatchAsync(
            new AdmitExecutionBatchRequest(
                "worker-b", "session-b", registration.SessionEpoch, 1,
                new[] { string.Empty },
                new[] { "default" },
                new[] { "test.echo" }),
            CancellationToken.None);
        blank.Results.Should().ContainSingle();
        blank.Results[0].Status.Should().Be(ExecutionAdmissionStatus.Rejected);
        blank.Results[0].Reason.Should().Be("invalid_admission_request");

        var exhausted = await workers.AdmitBatchAsync(
            new AdmitExecutionBatchRequest(
                "worker-b", "session-b", registration.SessionEpoch, 0,
                new[] { "run-1" },
                new[] { "default" },
                new[] { "test.echo" }),
            CancellationToken.None);
        exhausted.Results.Should().ContainSingle();
        exhausted.Results[0].Status.Should().Be(ExecutionAdmissionStatus.Retry);
        exhausted.Results[0].Reason.Should().Be("worker_capacity_exhausted");
    }

    [Fact]
    public async Task AdmitBatch_classifies_duplicate_envelopes_without_scheduling_one_attempt_twice()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        using var provider = services.BuildServiceProvider();

        var jobs = provider.GetRequiredService<JobControlPlane>();
        var workers = provider.GetRequiredService<WorkerControlPlane>();
        var registration = await workers.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-duplicate", "session-duplicate", null, "localhost", 2,
                new[] { "default" },
                new[] { "test.echo" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        var receipt = await SubmitAsync(jobs, "duplicate-envelope");

        var response = await workers.AdmitBatchAsync(
            new AdmitExecutionBatchRequest(
                registration.WorkerId,
                registration.SessionId,
                registration.SessionEpoch,
                2,
                new[] { receipt.Handle.JobId, receipt.Handle.JobId },
                new[] { "default" },
                new[] { "test.echo" }),
            CancellationToken.None);

        response.Results.Should().HaveCount(2);
        response.Results[0].Status.Should().Be(ExecutionAdmissionStatus.Admitted);
        response.Results[0].Job.Should().NotBeNull();
        response.Results[1].Status.Should().Be(ExecutionAdmissionStatus.Retry);
        response.Results[1].Reason.Should().Be("run_already_running");
        response.Results[1].Job.Should().BeNull();
    }

    [Fact]
    public async Task Batch_envelopes_are_admitted_and_executed_by_the_worker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.UseInProcessKubeJobWorkerTransport();
        using var provider = services.BuildServiceProvider();

        var jobs = provider.GetRequiredService<JobControlPlane>();
        var store = provider.GetRequiredService<InMemoryJobRuntimeStore>();
        var client = provider.GetRequiredService<IWorkerRuntimeClient>();
        var executions = new List<string>();
        var executionSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var registry = new JobHandlerRegistry(new[]
        {
            new RecordingInvoker("test.echo", executions, executionSignal)
        });
        var options = Options.Create(new KubeJobWorkerOptions
        {
            WorkerId = "worker-batch",
            Queues = new List<string> { "default" },
            MaxConcurrentJobs = 4,
            ClaimBatchSize = 4,
            // The pull claim loop is the broker-outage fallback; park it for
            // an hour so it cannot steal the runs before the batch admission.
            EmptyPollDelay = TimeSpan.FromHours(1),
            HeartbeatInterval = TimeSpan.FromMinutes(5),
            LeaseRenewalInterval = TimeSpan.FromMinutes(5),
            DrainTimeout = TimeSpan.FromSeconds(5)
        });
        using var claimTrigger = new WorkerClaimTrigger();
        using var worker = new WorkerRuntimeService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            registry,
            client,
            claimTrigger,
            options,
            NullLogger<WorkerRuntimeService>.Instance);

        await worker.StartAsync(CancellationToken.None);
        // Let registration and the first (empty) pull claim settle before
        // submitting, so the runs can only be picked up via batch admission.
        await Task.Delay(300);

        var first = await SubmitAsync(jobs, "batch-1");
        var second = await SubmitAsync(jobs, "batch-2");

        var outcomes = await worker.AdmitEnvelopesAsync(
            new[]
            {
                new ExecutionEnvelope
            {
                SchemaVersion = 3,
                EventId = "evt-1",
                Queue = "default",
                ExecutionLane = "default",
                ConsumerGroup = "default",
                RunId = first.Handle.JobId
            },
                new ExecutionEnvelope
            {
                SchemaVersion = 3,
                EventId = "evt-2",
                Queue = "default",
                ExecutionLane = "default",
                ConsumerGroup = "default",
                RunId = second.Handle.JobId
            }
            },
            CancellationToken.None);

        outcomes.Should().HaveCount(2);
        outcomes.Should().OnlyContain(outcome => outcome.Completion != null);

        // Wait for both admitted executions to complete durably.
        var results = await Task.WhenAll(
            outcomes.Select(outcome => outcome.Completion!));
        results.Should().OnlyContain(result => result.Status == ExecutionEnvelopeProcessingStatus.Completed);

        await executionSignal.Task.WaitAsync(TimeSpan.FromSeconds(10));
        // Handlers execute concurrently, so only set equality is guaranteed.
        executions.Should().BeEquivalentTo(first.Handle.JobId, second.Handle.JobId);

        var firstRun = await store.GetRunAsync(first.Handle.JobId, CancellationToken.None);
        var secondRun = await store.GetRunAsync(second.Handle.JobId, CancellationToken.None);
        firstRun!.Phase.Should().Be(JobPhase.Succeeded);
        secondRun!.Phase.Should().Be(JobPhase.Succeeded);

        await worker.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Batch_envelopes_return_retry_for_unconfigured_queues_and_capacity()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.UseInProcessKubeJobWorkerTransport();
        using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IWorkerRuntimeClient>();
        var store = provider.GetRequiredService<InMemoryJobRuntimeStore>();
        var jobs = provider.GetRequiredService<JobControlPlane>();

        var options = Options.Create(new KubeJobWorkerOptions
        {
            WorkerId = "worker-batch-2",
            Queues = new List<string> { "default" },
            MaxConcurrentJobs = 1,
            ClaimBatchSize = 1,
            EmptyPollDelay = TimeSpan.FromHours(1),
            HeartbeatInterval = TimeSpan.FromMinutes(5),
            LeaseRenewalInterval = TimeSpan.FromMinutes(5),
            DrainTimeout = TimeSpan.FromSeconds(5)
        });
        using var claimTrigger = new WorkerClaimTrigger();
        using var worker = new WorkerRuntimeService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            new JobHandlerRegistry(new[] { new RecordingInvoker("test.echo") }),
            client,
            claimTrigger,
            options,
            NullLogger<WorkerRuntimeService>.Instance);

        await worker.StartAsync(CancellationToken.None);
        await Task.Delay(300);

        var wrongQueue = await worker.AdmitEnvelopesAsync(
            new[] { new ExecutionEnvelope
            {
                SchemaVersion = 3,
                EventId = "evt-x",
                Queue = "mail",
                ExecutionLane = "default",
                ConsumerGroup = "default",
                RunId = "run-mail"
            } },
            CancellationToken.None);
        wrongQueue.Should().ContainSingle();
        wrongQueue[0].Completion.Should().BeNull();
        wrongQueue[0].Status.Should().Be(ExecutionEnvelopeProcessingStatus.Retry);
        wrongQueue[0].Reason.Should().Be("worker_not_configured_for_queue");

        // Exhaust the single execution slot with a parked handler, then verify
        // a subsequent batch reports capacity before admitting anything. The
        // run is submitted after the worker's first (empty) pull claim so only
        // the batch admission can pick it up.
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parkedOptions = Options.Create(new KubeJobWorkerOptions
        {
            WorkerId = "worker-batch-3",
            Queues = new List<string> { "default" },
            MaxConcurrentJobs = 1,
            ClaimBatchSize = 1,
            EmptyPollDelay = TimeSpan.FromHours(1),
            HeartbeatInterval = TimeSpan.FromMinutes(5),
            LeaseRenewalInterval = TimeSpan.FromMinutes(5),
            DrainTimeout = TimeSpan.FromSeconds(5)
        });
        using var parkedWorker = new WorkerRuntimeService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            new JobHandlerRegistry(new[] { new BlockingInvoker("test.echo", parked) }),
            client,
            new WorkerClaimTrigger(),
            parkedOptions,
            NullLogger<WorkerRuntimeService>.Instance);
        await parkedWorker.StartAsync(CancellationToken.None);
        await Task.Delay(300);

        var parkedRun = await SubmitAsync(jobs, "batch-parked");

        // Start the first batch (its handler parks) and wait until the run is
        // actually admitted before issuing the second batch.
        var firstOutcomes = await parkedWorker.AdmitEnvelopesAsync(
            new[] { new ExecutionEnvelope
            {
                SchemaVersion = 3,
                EventId = "evt-parked",
                Queue = "default",
                ExecutionLane = "default",
                ConsumerGroup = "default",
                RunId = parkedRun.Handle.JobId
            } },
            CancellationToken.None);
        firstOutcomes.Should().ContainSingle();
        var parkedCompletion = firstOutcomes[0].Completion;
        parkedCompletion.Should().NotBeNull();

        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var phase = (await store.GetRunAsync(parkedRun.Handle.JobId, CancellationToken.None))?.Phase;
            if (phase == JobPhase.Running)
            {
                break;
            }

            await Task.Delay(10);
        }

        var exhaustedOutcomes = await parkedWorker.AdmitEnvelopesAsync(
            new[] { new ExecutionEnvelope
            {
                SchemaVersion = 3,
                EventId = "evt-2",
                Queue = "default",
                ExecutionLane = "default",
                ConsumerGroup = "default",
                RunId = "run-2"
            } },
            CancellationToken.None);
        exhaustedOutcomes.Should().ContainSingle();
        exhaustedOutcomes[0].Completion.Should().BeNull();
        exhaustedOutcomes[0].Status.Should().Be(ExecutionEnvelopeProcessingStatus.Retry);
        exhaustedOutcomes[0].Reason.Should().Be("worker_capacity_exhausted");

        parked.SetResult();
        var firstResult = await parkedCompletion!.WaitAsync(TimeSpan.FromSeconds(10));
        firstResult.Status.Should().Be(ExecutionEnvelopeProcessingStatus.Completed);

        await parkedWorker.StopAsync(CancellationToken.None);
        await worker.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Schedule_fire_propagates_the_queue_execution_lane()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.ConfigureKubeJobQueueRouting(routing =>
        {
            routing.Defaults.Profile = ExecutionDeliveryProfile.Pull;
            routing.Queues["scheduled.q"] = new QueueDefinition
            {
                ExecutionLane = "lane-a",
                ConsumerGroup = "group-a"
            };
        });
        services.AddKubeJobServer();
        using var provider = services.BuildServiceProvider();

        var schedules = provider.GetRequiredService<ScheduleControlPlane>();
        var store = provider.GetRequiredService<IJobScheduleStore>();

        await schedules.CreateCronAsync(
            "schedule-lane",
            new UpsertCronScheduleRequest(
                "scheduled.job",
                "{}",
                "* * * * *",
                "UTC",
                "scheduled.q",
                0,
                MisfirePolicy.FireOnce,
                ScheduleConcurrencyPolicy.Allow,
                1,
                300,
                Enabled: true),
            CancellationToken.None);

        var schedule = await store.GetAsync("schedule-lane", CancellationToken.None);
        schedule.Should().NotBeNull();
        schedule!.ExecutionLane.Should().Be("lane-a");
        schedule.ConsumerGroup.Should().Be("group-a");

        // Fire one occurrence via the store, mirroring the reconciler. The
        // cron fires at the next minute boundary, so claim with a two-minute
        // horizon.
        var claims = await store.ClaimDueAsync(
            DateTimeOffset.UtcNow.AddMinutes(2),
            TimeSpan.FromMinutes(1),
            16,
            CancellationToken.None);
        var claim = claims.Should().ContainSingle().Subject;
        var planned = ScheduleReconciliationPlanner.Plan(claim.Schedule, DateTimeOffset.UtcNow);
        var run = await store.CommitFireAsync(
            new CommitScheduleFireCommand(
                claim.Schedule.Id,
                claim.ClaimToken,
                claim.ExpectedVersion,
                planned.ScheduledFor,
                planned.NextFireAt,
                planned.CreateRun,
                ScheduleReconcilerService.CreateOccurrenceId(claim.Schedule.Id, planned.ScheduledFor),
                $"schedule:{claim.Schedule.Id}:{planned.ScheduledFor.UtcTicks}"),
            CancellationToken.None);

        run.Should().NotBeNull();
        run!.ExecutionLane.Should().Be("lane-a");
        run.ConsumerGroup.Should().Be("group-a");
        run.Queue.Should().Be("scheduled.q");
    }

    [Fact]
    public void Cancel_queue_name_is_stable_for_a_worker_id()
    {
        // The consumer names the cancel queue by the stable WorkerId, so a
        // restart (new SessionId) reuses the same physical queue instead of
        // creating a new ephemeral one per session.
        var options = new RabbitMqExecutionOptions();
        var first = options.GetCancelQueueName("default", "worker-prod-01");
        var second = options.GetCancelQueueName("default", "worker-prod-01");

        first.Should().Be(second);
        first.Should().StartWith("kubejob.execution.default.cancel.");
        first.Should().NotContain("session");
    }

    private static async Task<JobSubmissionReceipt> SubmitAsync(JobControlPlane jobs, string idempotencyKey) =>
        await jobs.SubmitAsync(
            new EnqueueJobRequest(
                "test.echo",
                "{}",
                "default",
                0,
                DateTimeOffset.UtcNow,
                idempotencyKey,
                null,
                1,
                300),
            CancellationToken.None);

    private static async Task CompleteAsSucceededAsync(
        WorkerControlPlane workers,
        string runId,
        RegisterWorkerSessionResponse registration)
    {
        var claimed = await ClaimAsync(workers, runId, registration);
        claimed.Jobs.Should().ContainSingle();
        var job = claimed.Jobs[0];
        var completed = await workers.CompleteAsync(
            new CompleteAttemptRequest(
                "worker-a",
                "session-a",
                registration.SessionEpoch,
                job.RunId,
                job.AttemptId,
                job.AttemptNumber,
                job.LeaseToken,
                JobAttemptOutcome.Succeeded),
            CancellationToken.None);
        completed.Accepted.Should().BeTrue();
    }

    private static async Task<ClaimJobsResponse> ClaimAsync(
        WorkerControlPlane workers,
        string runId,
        RegisterWorkerSessionResponse registration) =>
        await workers.ClaimAsync(
            new ClaimJobsRequest(
                "worker-a",
                "session-a",
                registration.SessionEpoch,
                10,
                new[] { "default" },
                new[] { "test.echo" },
                RunIds: new[] { runId }),
            CancellationToken.None);

    private sealed class RecordingInvoker : IJobHandlerInvoker
    {
        private readonly List<string> _executions;
        private readonly TaskCompletionSource? _signal;

        public RecordingInvoker(string jobKey, List<string>? executions = null, TaskCompletionSource? signal = null)
        {
            JobKey = jobKey;
            _executions = executions ?? new List<string>();
            _signal = signal;
        }

        public string JobKey { get; }

        public Type PayloadType => typeof(object);

        public ValueTask InvokeAsync(
            IServiceProvider serviceProvider,
            string payloadJson,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            lock (_executions)
            {
                _executions.Add(context.RunId);
                _signal?.TrySetResult();
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class BlockingInvoker : IJobHandlerInvoker
    {
        private readonly TaskCompletionSource _release;

        public BlockingInvoker(string jobKey, TaskCompletionSource release)
        {
            JobKey = jobKey;
            _release = release;
        }

        public string JobKey { get; }

        public Type PayloadType => typeof(object);

        public async ValueTask InvokeAsync(
            IServiceProvider serviceProvider,
            string payloadJson,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            await _release.Task.WaitAsync(cancellationToken);
        }
    }
}
