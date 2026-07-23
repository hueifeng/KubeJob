using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class InMemoryJobRuntimeStoreTests
{
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
            TimeSpan.Zero,
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
            TimeSpan.Zero,
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
    public async Task Completion_from_expired_attempt_is_rejected_after_reassignment()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(NewCommand(maxAttempts: 2), CancellationToken.None);
        var workerA = await RegisterAsync(store, "worker-a", "session-a");
        var workerB = await RegisterAsync(store, "worker-b", "session-b");
        var first = (await store.ClaimAsync(
            NewClaim(workerA),
            TimeSpan.FromSeconds(1),
            1,
            CancellationToken.None)).Single();

        await store.RequeueExpiredLeasesAsync(
            first.LeaseExpiresAt.AddSeconds(1),
            TimeSpan.Zero,
            10,
            CancellationToken.None);
        var second = (await store.ClaimAsync(
            NewClaim(workerB),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();

        var stale = await store.CompleteAsync(
            NewCompletion(workerA, first, JobAttemptOutcome.Succeeded),
            TimeSpan.Zero,
            CancellationToken.None);
        var current = await store.CompleteAsync(
            NewCompletion(workerB, second, JobAttemptOutcome.Succeeded),
            TimeSpan.Zero,
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

        var accepted = await store.RequestCancelAsync(run.Id, "not needed", CancellationToken.None);
        var claimed = await store.ClaimAsync(
            NewClaim(worker),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None);
        var snapshot = await store.GetRunAsync(run.Id, CancellationToken.None);

        accepted.Should().BeTrue();
        claimed.Should().BeEmpty();
        snapshot!.Phase.Should().Be(JobPhase.Canceled);
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
            message.Id,
            "broker unavailable",
            DateTimeOffset.UtcNow.AddSeconds(-1),
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

    private static SubmitJobCommand NewCommand(
        string? idempotencyKey = null,
        int maxAttempts = 1,
        string? concurrencyKey = null) => new(
        "mail.send",
        "{\"to\":\"user@example.com\"}",
        "default",
        0,
        DateTimeOffset.UtcNow,
        idempotencyKey,
        concurrencyKey,
        maxAttempts,
        300);

    private static async Task<WorkerSessionRecord> RegisterAsync(
        InMemoryJobRuntimeStore store,
        string workerId,
        string sessionId) => await store.RegisterAsync(
        new RegisterWorkerSessionRequest(
            workerId,
            sessionId,
            "test",
            "localhost",
            1,
            new[] { "default" },
            new[] { "mail.send" },
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
