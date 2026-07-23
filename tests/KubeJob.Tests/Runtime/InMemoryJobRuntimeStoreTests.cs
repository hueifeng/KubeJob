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

    private static SubmitJobCommand NewCommand(
        string? idempotencyKey = null,
        int maxAttempts = 1) => new(
        "mail.send",
        "{\"to\":\"user@example.com\"}",
        "default",
        0,
        DateTimeOffset.UtcNow,
        idempotencyKey,
        null,
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
