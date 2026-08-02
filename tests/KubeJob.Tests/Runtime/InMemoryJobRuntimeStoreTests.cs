using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class InMemoryJobRuntimeStoreTests
{
    private static readonly RetryPolicy TestRetryPolicy =
        new(BackoffStrategy.Fixed, TimeSpan.Zero, TimeSpan.Zero);

    [Fact]
    public async Task Submit_with_same_idempotency_key_returns_same_logical_run()
    {
        var store = new InMemoryJobRuntimeStore();
        var command = NewCommand(idempotencyKey: "order:42");

        var first = await store.SubmitAsync(command, CancellationToken.None);
        var second = await store.SubmitAsync(command, CancellationToken.None);

        second.Existing.Should().BeTrue();
        second.Run.Id.Should().Be(first.Run.Id);
    }

    [Fact]
    public async Task Concurrent_workers_cannot_claim_the_same_run()
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(NewCommand(), CancellationToken.None)).Run;
        var workerA = await RegisterAsync(store, "worker-a", "session-a");
        var workerB = await RegisterAsync(store, "worker-b", "session-b");

        var claims = await Task.WhenAll(
            store.ClaimAsync(NewClaim(workerA), TimeSpan.FromSeconds(30), 1, CancellationToken.None).AsTask(),
            store.ClaimAsync(NewClaim(workerB), TimeSpan.FromSeconds(30), 1, CancellationToken.None).AsTask());

        claims.Sum(x => x.Count).Should().Be(1);
        claims.SelectMany(x => x).Single().RunId.Should().Be(run.Id);
    }

    [Fact]
    public async Task Targeted_claim_admits_only_the_run_named_by_an_execution_envelope()
    {
        var store = new InMemoryJobRuntimeStore();
        var target = (await store.SubmitAsync(
            NewCommand(idempotencyKey: "target"),
            CancellationToken.None)).Run;
        var other = (await store.SubmitAsync(
            NewCommand(idempotencyKey: "other"),
            CancellationToken.None)).Run;
        var worker = await RegisterAsync(store, "worker", "session");

        var claim = await store.ClaimAsync(
            NewClaim(worker) with { RunIds = new[] { target.Id } },
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None);

        claim.Should().ContainSingle();
        claim.Single().RunId.Should().Be(target.Id);
        other.Phase.Should().Be(JobPhase.Pending);
    }

    [Fact]
    public async Task Runs_with_the_same_concurrency_key_do_not_run_together()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(concurrencyKey: "tenant:42"), CancellationToken.None);
        await store.SubmitAsync(NewCommand(concurrencyKey: "tenant:42"), CancellationToken.None);
        var workerA = await RegisterAsync(store, "worker-a", "session-a");
        var workerB = await RegisterAsync(store, "worker-b", "session-b");

        var claims = await Task.WhenAll(
            store.ClaimAsync(NewClaim(workerA), TimeSpan.FromSeconds(30), 1, CancellationToken.None).AsTask(),
            store.ClaimAsync(NewClaim(workerB), TimeSpan.FromSeconds(30), 1, CancellationToken.None).AsTask());

        claims.Sum(x => x.Count).Should().Be(1);
    }

    [Fact]
    public async Task Retry_creates_a_new_attempt_for_the_same_run()
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(NewCommand(maxAttempts: 2), CancellationToken.None)).Run;
        var worker = await RegisterAsync(store, "worker", "session");
        var first = (await store.ClaimAsync(
            NewClaim(worker),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();

        var failed = await store.CompleteAsync(
            NewCompletion(worker, first, JobAttemptOutcome.RetryableFailure),
            TestRetryPolicy,
            CancellationToken.None);
        var second = (await store.ClaimAsync(
            NewClaim(worker),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();

        failed.Requeued.Should().BeTrue();
        second.RunId.Should().Be(run.Id);
        second.AttemptNumber.Should().Be(2);
        second.AttemptId.Should().NotBe(first.AttemptId);
    }

    [Fact]
    public async Task Completion_from_expired_attempt_is_rejected_before_reaper_runs()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(maxAttempts: 2), CancellationToken.None);
        var worker = await RegisterAsync(store, "worker", "session");
        var claim = (await store.ClaimAsync(
            NewClaim(worker),
            TimeSpan.FromSeconds(-1),
            1,
            CancellationToken.None)).Single();

        var completion = await store.CompleteAsync(
            NewCompletion(worker, claim, JobAttemptOutcome.Succeeded),
            TestRetryPolicy,
            CancellationToken.None);

        completion.Accepted.Should().BeFalse();
    }

    [Fact]
    public async Task Expired_attempt_cannot_be_renewed()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(maxAttempts: 2), CancellationToken.None);
        var worker = await RegisterAsync(store, "worker", "session");
        var claim = (await store.ClaimAsync(
            NewClaim(worker),
            TimeSpan.FromSeconds(-1),
            1,
            CancellationToken.None)).Single();

        var renewal = await store.RenewLeasesAsync(
            new RenewLeasesRequest(
                worker.WorkerId,
                worker.SessionId,
                worker.Epoch,
                new[] { new LeaseRenewal(claim.AttemptId, claim.LeaseToken) }),
            TimeSpan.FromSeconds(30),
            CancellationToken.None);

        renewal.Single().Renewed.Should().BeFalse();
    }

    [Fact]
    public async Task Lease_reaper_fires_terminal_actions_when_retry_budget_is_exhausted()
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(
            NewCommand(
                maxAttempts: 1,
                continuation: new Continuation
                {
                    JobKey = "mail.followup",
                    Trigger = ContinuationTrigger.OnAnyTerminal
                },
                compensation: new Compensation
                {
                    JobKey = "mail.compensate"
                }),
            CancellationToken.None)).Run;
        var worker = await RegisterAsync(store, "reaper-worker", "reaper-session");
        (await store.ClaimAsync(
            NewClaim(worker),
            TimeSpan.FromSeconds(-1),
            1,
            CancellationToken.None)).Should().ContainSingle();

        var reconciled = await store.RequeueExpiredLeasesAsync(
            DateTimeOffset.UtcNow,
            TestRetryPolicy,
            10,
            CancellationToken.None);

        reconciled.Should().Be(1);
        (await store.GetRunAsync(run.Id, CancellationToken.None))!.Phase.Should().Be(JobPhase.Dead);

        var followUpWorker = await RegisterAsync(
            store,
            "reaper-followup-worker",
            "reaper-followup-session",
            maxConcurrency: 2,
            capabilities: new[] { "mail.followup", "mail.compensate" });
        var followUps = await store.ClaimAsync(
            NewClaim(followUpWorker) with
            {
                Capabilities = new[] { "mail.followup", "mail.compensate" },
                AvailableSlots = 2
            },
            TimeSpan.FromSeconds(30),
            2,
            CancellationToken.None);

        followUps.Select(job => job.JobKey)
            .Should().BeEquivalentTo(new[] { "mail.followup", "mail.compensate" });
        var followUpRuns = await Task.WhenAll(
            followUps.Select(job => store.GetRunAsync(job.RunId, CancellationToken.None).AsTask()));
        followUpRuns.All(candidate => candidate is not null).Should().BeTrue();
        followUpRuns.Single(run => run!.RelationKind == RunRelationKind.Continuation)!
            .ParentRunId.Should().Be(run.Id);
        followUpRuns.Single(run => run!.RelationKind == RunRelationKind.Compensation)!
            .ParentRunId.Should().Be(run.Id);
    }

    [Fact]
    public async Task Completion_from_expired_attempt_is_rejected_after_reassignment()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(maxAttempts: 2), CancellationToken.None);
        var workerA = await RegisterAsync(store, "worker-a", "session-a");
        var workerB = await RegisterAsync(store, "worker-b", "session-b");
        var first = (await store.ClaimAsync(
            NewClaim(workerA),
            TimeSpan.FromSeconds(-1),
            1,
            CancellationToken.None)).Single();

        await store.RequeueExpiredLeasesAsync(
            DateTimeOffset.UtcNow,
            TestRetryPolicy,
            10,
            CancellationToken.None);
        var second = (await store.ClaimAsync(
            NewClaim(workerB),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();

        var stale = await store.CompleteAsync(
            NewCompletion(workerA, first, JobAttemptOutcome.Succeeded),
            TestRetryPolicy,
            CancellationToken.None);
        var current = await store.CompleteAsync(
            NewCompletion(workerB, second, JobAttemptOutcome.Succeeded),
            TestRetryPolicy,
            CancellationToken.None);

        stale.Accepted.Should().BeFalse();
        current.Accepted.Should().BeTrue();
        current.Phase.Should().Be(JobPhase.Succeeded);
    }

    [Fact]
    public async Task Re_registering_worker_invalidates_previous_session_epoch()
    {
        var store = new InMemoryJobRuntimeStore();
        var oldSession = await RegisterAsync(store, "worker", "session-old");
        var newSession = await RegisterAsync(store, "worker", "session-new");

        var oldHeartbeat = await store.HeartbeatAsync(
            new WorkerHeartbeatRequest(
                oldSession.WorkerId,
                oldSession.SessionId,
                oldSession.Epoch,
                1,
                WorkerSessionState.Ready),
            CancellationToken.None);

        newSession.Epoch.Should().Be(oldSession.Epoch + 1);
        oldHeartbeat.Should().BeFalse();
    }

    [Fact]
    public async Task Retrying_same_session_registration_preserves_epoch()
    {
        var store = new InMemoryJobRuntimeStore();
        var first = await RegisterAsync(store, "worker", "same-session");
        var retry = await RegisterAsync(store, "worker", "same-session");

        retry.Epoch.Should().Be(first.Epoch);
        retry.SessionId.Should().Be(first.SessionId);
    }

    [Fact]
    public async Task Canceling_pending_run_prevents_claim()
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(NewCommand(), CancellationToken.None)).Run;
        var worker = await RegisterAsync(store, "worker", "session");

        var accepted = await store.RequestCancelAsync(run.Id, "not needed", null, CancellationToken.None);
        var claimed = await store.ClaimAsync(
            NewClaim(worker),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None);
        var snapshot = await store.GetRunAsync(run.Id, CancellationToken.None);

        accepted.Requested.Should().BeTrue();
        claimed.Should().BeEmpty();
        snapshot!.Phase.Should().Be(JobPhase.Canceled);
    }

    [Fact]
    public async Task Canceling_running_run_does_not_fire_terminal_actions()
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(
            NewCommand(
                continuation: new Continuation
                {
                    JobKey = "mail.followup",
                    Trigger = ContinuationTrigger.OnAnyTerminal
                },
                compensation: new Compensation
                {
                    JobKey = "mail.compensate"
                }),
            CancellationToken.None)).Run;
        var worker = await RegisterAsync(
            store,
            "cancel-worker",
            "cancel-session",
            maxConcurrency: 1,
            capabilities: new[] { "mail.send" });
        var claim = (await store.ClaimAsync(
            NewClaim(worker),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();

        (await store.RequestCancelAsync(
            run.Id,
            "operator canceled",
            null,
            CancellationToken.None)).Requested.Should().BeTrue();
        var completion = await store.CompleteAsync(
            NewCompletion(worker, claim, JobAttemptOutcome.Canceled),
            TestRetryPolicy,
            CancellationToken.None);
        var runs = await store.GetRunsAsync(
            new DashboardRunQuery(PageSize: 100),
            CancellationToken.None);

        completion.Phase.Should().Be(JobPhase.Canceled);
        runs.TotalCount.Should().Be(1);
        runs.Items.Should().ContainSingle(item => item.Id == run.Id);
    }

    [Fact]
    public async Task Failed_outbox_message_becomes_available_for_retry()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(), CancellationToken.None);
        var firstClaim = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);
        var message = firstClaim.Single();

        await store.MarkFailedAsync(
            new OutboxFailure(
                message.Id,
                message.ClaimToken!,
                "broker unavailable",
                DateTimeOffset.UtcNow.AddSeconds(-1)),
            CancellationToken.None);
        var retryClaim = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);

        retryClaim.Single().Id.Should().Be(message.Id);
        retryClaim.Single().PublishAttempts.Should().Be(2);
    }

    [Fact]
    public async Task Broker_retry_budget_requeues_pending_run_through_durable_outbox()
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(NewCommand(), CancellationToken.None)).Run;
        var requeueAt = DateTimeOffset.UtcNow.AddMinutes(1);

        var scheduled = await store.RequeueWorkAvailableAsync(
            run.Id,
            requeueAt,
            CancellationToken.None);
        var messages = await store.ClaimPendingAsync(
            requeueAt,
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);

        scheduled.Should().BeTrue();
        messages.Count(message => message.PayloadJson.Contains(run.Id)).Should().Be(2);

        var canceled = await store.RequestCancelAsync(
            run.Id,
            "cancel before reconciliation",
            null,
            CancellationToken.None);
        var scheduledAfterCancel = await store.RequeueWorkAvailableAsync(
            run.Id,
            requeueAt,
            CancellationToken.None);

        canceled.Requested.Should().BeTrue();
        scheduledAfterCancel.Should().BeFalse();
    }

    [Fact]
    public async Task Abandoned_publishing_message_is_reclaimed_after_claim_lease()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(), CancellationToken.None);
        var firstNow = DateTimeOffset.UtcNow.AddSeconds(1);
        var firstClaim = await store.ClaimPendingAsync(
            firstNow,
            TimeSpan.FromSeconds(10),
            10,
            CancellationToken.None);

        var beforeExpiry = await store.ClaimPendingAsync(
            firstNow.AddSeconds(9),
            TimeSpan.FromSeconds(10),
            10,
            CancellationToken.None);
        var afterExpiry = await store.ClaimPendingAsync(
            firstNow.AddSeconds(11),
            TimeSpan.FromSeconds(10),
            10,
            CancellationToken.None);

        beforeExpiry.Should().BeEmpty();
        afterExpiry.Single().Id.Should().Be(firstClaim.Single().Id);
        afterExpiry.Single().PublishAttempts.Should().Be(2);
    }

    [Fact]
    public async Task Stale_outbox_publisher_cannot_overwrite_a_reclaimed_message()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(), CancellationToken.None);
        var first = (await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(10),
            10,
            CancellationToken.None)).Single();
        var firstClaimToken = first.ClaimToken!;

        var second = (await store.ClaimPendingAsync(
            first.AvailableAt.AddSeconds(1),
            TimeSpan.FromSeconds(10),
            10,
            CancellationToken.None)).Single();

        await store.MarkFailedAsync(
            new OutboxFailure(
                first.Id,
                firstClaimToken,
                "stale publisher",
                DateTimeOffset.UtcNow.AddSeconds(-1)),
            CancellationToken.None);

        second.State.Should().Be(OutboxDeliveryState.Publishing);
        second.ClaimToken.Should().NotBe(firstClaimToken);
        second.LastError.Should().BeNull();
    }

    [Fact]
    public async Task Batch_outbox_publication_only_completes_matching_claim_tokens()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(idempotencyKey: "one"), CancellationToken.None);
        await store.SubmitAsync(NewCommand(idempotencyKey: "two"), CancellationToken.None);
        var claimed = (await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None)).ToArray();

        await store.MarkPublishedAsync(
            new[]
            {
                new OutboxPublication(claimed[0].Id, claimed[0].ClaimToken!),
                new OutboxPublication(claimed[1].Id, "wrong-token")
            },
            DateTimeOffset.UtcNow,
            CancellationToken.None);

        claimed[0].State.Should().Be(OutboxDeliveryState.Published);
        claimed[1].State.Should().Be(OutboxDeliveryState.Publishing);
    }

    [Fact]
    public async Task Batch_submission_does_not_leave_rows_when_a_later_idempotency_conflicts()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(
            NewCommand(idempotencyKey: "already-exists"),
            CancellationToken.None);

        var invalid = async () => await store.SubmitBatchAsync(new[]
        {
            NewCommand(idempotencyKey: "new-row"),
            NewCommand(idempotencyKey: "already-exists") with
            {
                PayloadJson = "{\"to\":\"different\"}"
            }
        }, CancellationToken.None);

        await invalid.Should().ThrowAsync<IdempotencyConflictException>();
        (await store.GetByIdempotencyKeyAsync("new-row", CancellationToken.None))
            .Should().BeNull();
    }

    [Fact]
    public async Task Outbox_dispatch_respects_batch_size()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(idempotencyKey: "batch-one"), CancellationToken.None);
        await store.SubmitAsync(NewCommand(idempotencyKey: "batch-two"), CancellationToken.None);
        await store.SubmitAsync(NewCommand(idempotencyKey: "batch-three"), CancellationToken.None);

        var dispatched = await store.DispatchOnceAsync(
            TimeSpan.FromMinutes(1),
            TimeSpan.Zero,
            2,
            (_, _) => ValueTask.CompletedTask,
            CancellationToken.None);

        dispatched.DispatchedIds.Should().HaveCount(2);
        var remaining = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromMinutes(1),
            10,
            CancellationToken.None);
        remaining.Should().ContainSingle();
    }

    [Fact]
    public async Task Permanent_outbox_failure_is_abandoned_instead_of_retried_forever()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(), CancellationToken.None);

        var result = await store.DispatchOnceAsync(
            TimeSpan.FromMinutes(1),
            TimeSpan.Zero,
            10,
            (_, _) => throw new PermanentOutboxException("invalid event"),
            CancellationToken.None);

        result.Abandoned.Should().ContainSingle();
        result.FailedIds.Should().BeEmpty();

        var next = await store.DispatchOnceAsync(
            TimeSpan.FromMinutes(1),
            TimeSpan.Zero,
            10,
            (_, _) => ValueTask.CompletedTask,
            CancellationToken.None);
        next.DispatchedIds.Should().BeEmpty();
        next.Abandoned.Should().BeEmpty();
    }

    [Fact]
    public async Task Runtime_maintenance_removes_published_outbox_and_unkeyed_terminal_runs()
    {
        var store = new InMemoryJobRuntimeStore();
        var unkeyedRun = (await store.SubmitAsync(NewCommand(), CancellationToken.None)).Run;
        var keyedRun = (await store.SubmitAsync(
            NewCommand(idempotencyKey: "retain-me"),
            CancellationToken.None)).Run;
        var worker = await RegisterAsync(store, "maintenance-worker", "maintenance-session");
        var claim = (await store.ClaimAsync(
            NewClaim(worker),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        await store.CompleteAsync(
            NewCompletion(worker, claim, JobAttemptOutcome.Succeeded),
            TestRetryPolicy,
            CancellationToken.None);
        await store.DispatchOnceAsync(
            TimeSpan.FromMinutes(1),
            TimeSpan.Zero,
            10,
            (_, _) => ValueTask.CompletedTask,
            CancellationToken.None);

        var maintenance = (IJobRuntimeMaintenanceStore)store;
        var outboxDeleted = await maintenance.DeletePublishedOutboxAsync(
            DateTimeOffset.UtcNow.AddMinutes(1),
            100,
            CancellationToken.None);
        var terminalDeleted = await maintenance.DeleteUnkeyedTerminalRunsAsync(
            DateTimeOffset.UtcNow.AddMinutes(1),
            100,
            CancellationToken.None);

        outboxDeleted.Should().BeGreaterThan(0);
        terminalDeleted.Should().Be(1);
        (await store.GetRunAsync(unkeyedRun.Id, CancellationToken.None)).Should().BeNull();
        (await store.GetRunAsync(keyedRun.Id, CancellationToken.None)).Should().NotBeNull();
    }

    private static SubmitJobCommand NewCommand(
        string? idempotencyKey = null,
        int maxAttempts = 1,
        string? concurrencyKey = null,
        RetryPolicy? retryPolicy = null,
        Continuation? continuation = null,
        Compensation? compensation = null) => new(
        "mail.send",
        "{\"to\":\"user@example.com\"}",
        "default",
        0,
        DateTimeOffset.UtcNow,
        idempotencyKey,
        concurrencyKey,
        maxAttempts,
        300,
        RetryPolicy: retryPolicy,
        Continuation: continuation,
        Compensation: compensation);

    private static async Task<WorkerSessionRecord> RegisterAsync(
        InMemoryJobRuntimeStore store,
        string workerId,
        string sessionId,
        int maxConcurrency = 1,
        IReadOnlyList<string>? capabilities = null) => await store.RegisterAsync(
        new RegisterWorkerSessionRequest(
            workerId,
            sessionId,
            "test",
            "localhost",
            maxConcurrency,
            new[] { "default" },
            capabilities ?? new[] { "mail.send" },
            new Dictionary<string, string>()),
        CancellationToken.None);

    private static ClaimJobsRequest NewClaim(WorkerSessionRecord session) => new(
        session.WorkerId,
        session.SessionId,
        session.Epoch,
        1,
        new[] { "default" },
        new[] { "mail.send" });

    private static CompleteAttemptRequest NewCompletion(
        WorkerSessionRecord session,
        ClaimedJob job,
        JobAttemptOutcome outcome) => new(
        session.WorkerId,
        session.SessionId,
        session.Epoch,
        job.RunId,
        job.AttemptId,
        job.AttemptNumber,
        job.LeaseToken,
        outcome);
}
