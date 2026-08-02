using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class OrderingBacklogQueryTests
{
    [Fact]
    public async Task GetOrderingBacklog_reports_blocked_successors_and_active_keys_per_queue()
    {
        var store = new InMemoryJobRuntimeStore();
        var past = DateTimeOffset.UtcNow.AddSeconds(-30);

        // Three KeyOrdered runs on the same key on "default": the head (lowest
        // OrderingSequence) is claimable, the two successors are blocked behind
        // a non-terminal same-key predecessor.
        await store.SubmitAsync(KeyOrdered("default", "tenant:42", past), CancellationToken.None);
        await store.SubmitAsync(KeyOrdered("default", "tenant:42", past), CancellationToken.None);
        await store.SubmitAsync(KeyOrdered("default", "tenant:42", past), CancellationToken.None);

        // A second key on the same queue is its own head (not blocked) but
        // counts as a second active key.
        await store.SubmitAsync(KeyOrdered("default", "tenant:99", past), CancellationToken.None);

        // A Parallel run must not appear in the ordering backlog even if it
        // shares the queue and key.
        await store.SubmitAsync(Parallel("default", "tenant:42", past), CancellationToken.None);

        // A KeyOrdered run on a different queue is its own head (not blocked).
        await store.SubmitAsync(KeyOrdered("orders", "order:1", past), CancellationToken.None);

        var backlog = await store.GetOrderingBacklogAsync(CancellationToken.None);

        var defaultSample = backlog.Single(sample => sample.Queue == "default");
        defaultSample.BlockedRuns.Should().Be(2);
        defaultSample.ActiveKeys.Should().Be(2);
        defaultSample.OldestBlockedAgeSeconds.Should().BeGreaterThan(0).And.BeLessThan(120);

        var ordersSample = backlog.Single(sample => sample.Queue == "orders");
        ordersSample.BlockedRuns.Should().Be(0);
        ordersSample.ActiveKeys.Should().Be(1);
        ordersSample.OldestBlockedAgeSeconds.Should().Be(0);
    }

    [Fact]
    public async Task GetOrderingBacklog_clears_a_key_once_its_head_runs_to_terminal()
    {
        var store = new InMemoryJobRuntimeStore();
        var past = DateTimeOffset.UtcNow.AddSeconds(-30);
        var worker = await RegisterAsync(store);

        // Two KeyOrdered runs on one key: head is claimable, one blocked successor.
        var head = (await store.SubmitAsync(
            KeyOrdered("default", "tenant:42", past), CancellationToken.None)).Run;
        await store.SubmitAsync(KeyOrdered("default", "tenant:42", past), CancellationToken.None);

        var before = await store.GetOrderingBacklogAsync(CancellationToken.None);
        before.Single(sample => sample.Queue == "default").BlockedRuns.Should().Be(1);

        // Claim and complete the head; the successor becomes the new head and is
        // no longer blocked.
        var claim = (await store.ClaimAsync(
            NewClaim(worker, new[] { head.Id }),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();
        await store.CompleteAsync(
            NewCompletion(worker, claim, JobAttemptOutcome.Succeeded),
            new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.Zero, TimeSpan.Zero),
            CancellationToken.None);

        var after = await store.GetOrderingBacklogAsync(CancellationToken.None);
        after.Single(sample => sample.Queue == "default").BlockedRuns.Should().Be(0);
    }

    private static SubmitJobCommand KeyOrdered(string queue, string concurrencyKey, DateTimeOffset availableAt) =>
        NewCommand(queue, concurrencyKey, ExecutionOrderingMode.KeyOrdered, availableAt);

    private static SubmitJobCommand Parallel(string queue, string concurrencyKey, DateTimeOffset availableAt) =>
        NewCommand(queue, concurrencyKey, ExecutionOrderingMode.Parallel, availableAt);

    private static SubmitJobCommand NewCommand(
        string queue,
        string concurrencyKey,
        ExecutionOrderingMode mode,
        DateTimeOffset availableAt) => new(
        "mail.send",
        "{}",
        queue,
        0,
        availableAt,
        IdempotencyKey: null,
        ConcurrencyKey: concurrencyKey,
        MaxAttempts: 1,
        TimeoutSeconds: 300,
        ScheduleId: null,
        ScheduledFor: null,
        DeliveryTarget: new DeliveryTarget(
            ExecutionDeliveryProfile.Pull,
            "default",
            null,
            "default",
            mode));

    private static async Task<WorkerSessionRecord> RegisterAsync(InMemoryJobRuntimeStore store) =>
        await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker",
                "session",
                "test",
                "localhost",
                1,
                new[] { "default" },
                new[] { "mail.send" },
                new Dictionary<string, string>()),
            CancellationToken.None);

    private static ClaimJobsRequest NewClaim(WorkerSessionRecord session, IReadOnlyList<string> runIds) => new(
        session.WorkerId,
        session.SessionId,
        session.Epoch,
        1,
        new[] { "default" },
        new[] { "mail.send" },
        runIds);

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