using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// PostgresManaged StrictFIFO ordering tests. BrokerNative ordering is owned by
/// the selected transport and does not pass through this database claim gate.
/// </summary>
public sealed class StrictFifoOrderingTests
{
    private static readonly TimeSpan ClaimTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Inflight_run_on_same_queue_blocks_subsequent_claims()
    {
        var store = new InMemoryJobRuntimeStore();
        var r1Id = await SubmitAndClaim(store, "q", ExecutionOrderingMode.StrictFifo, "w1");
        await store.SubmitAsync(CreateCommand("q", ExecutionOrderingMode.StrictFifo), CancellationToken.None);

        var empty = await Claim(store, "q", "w2", max: 1);
        empty.Should().BeEmpty("StrictFIFO successor must be blocked by inflight predecessor");

        await Complete(store, r1Id, "w1", JobAttemptOutcome.Succeeded);
        var claims = await Claim(store, "q", "w2", max: 1);
        claims.Should().ContainSingle("Run 2 should advance after predecessor completes");
    }

    [Fact]
    public async Task Different_queue_not_blocked()
    {
        var store = new InMemoryJobRuntimeStore();
        await SubmitAndClaim(store, "q-sf", ExecutionOrderingMode.StrictFifo, "w1");
        var r2Id = await SubmitAndClaim(store, "q-parallel", ExecutionOrderingMode.Parallel, "w2");
        r2Id.Should().NotBeNullOrEmpty("Different queue should not be blocked by StrictFIFO");
    }

    [Fact]
    public async Task KeyOrdered_on_different_queue_runs_parallel_to_StrictFifo()
    {
        var store = new InMemoryJobRuntimeStore();
        var aId = await SubmitAndClaim(store, "q", ExecutionOrderingMode.KeyOrdered, "w1", "key-A");
        var bId = await SubmitAndClaim(store, "other-q", ExecutionOrderingMode.StrictFifo, "w2");

        aId.Should().NotBeNullOrEmpty();
        bId.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Terminal_failure_unblocks_queue()
    {
        var store = new InMemoryJobRuntimeStore();
        var r1Id = await SubmitAndClaim(store, "q", ExecutionOrderingMode.StrictFifo, "w1");
        await Complete(store, r1Id, "w1", JobAttemptOutcome.PermanentFailure);

        await store.SubmitAsync(CreateCommand("q", ExecutionOrderingMode.StrictFifo), CancellationToken.None);
        var claims = await Claim(store, "q", "w2", max: 1);
        claims.Should().ContainSingle("Terminal predecessor should unblock the queue");
    }

    private static async Task<string> SubmitAndClaim(
        InMemoryJobRuntimeStore store,
        string queue,
        ExecutionOrderingMode orderingMode,
        string workerId,
        string? concurrencyKey = null)
    {
        await store.SubmitAsync(CreateCommand(queue, orderingMode, concurrencyKey), CancellationToken.None);
        var claimed = await Claim(store, queue, workerId, max: 1);
        claimed.Should().HaveCount(1, $"Expected to claim a {orderingMode} run on queue '{queue}'");
        return claimed[0].RunId;
    }

    private static async Task<IReadOnlyList<ClaimedJob>> Claim(
        InMemoryJobRuntimeStore store,
        string queue,
        string workerId,
        int max)
    {
        await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                workerId,
                $"session-{workerId}",
                "test",
                "test-host",
                max,
                new[] { queue },
                new[] { "test" },
                new Dictionary<string, string>(),
                "default"),
            CancellationToken.None);
        return await store.ClaimAsync(
            new ClaimJobsRequest(
                WorkerId: workerId,
                SessionId: $"session-{workerId}",
                SessionEpoch: 1,
                AvailableSlots: max,
                Queues: new[] { queue },
                Capabilities: new[] { "test" }),
            ClaimTimeout,
            max,
            CancellationToken.None);
    }

    private static async Task Complete(
        InMemoryJobRuntimeStore store,
        string runId,
        string workerId,
        JobAttemptOutcome outcome)
    {
        var attempts = await store.GetAttemptsAsync(runId, CancellationToken.None);
        var attempt = attempts.Single(a => a.Phase == JobAttemptPhase.Running);
        await store.CompleteAsync(
            new CompleteAttemptRequest(
                WorkerId: workerId,
                SessionId: attempt.SessionId,
                SessionEpoch: attempt.SessionEpoch,
                RunId: runId,
                AttemptId: attempt.Id,
                AttemptNumber: attempt.AttemptNumber,
                LeaseToken: attempt.LeaseToken,
                Outcome: outcome),
            new RetryPolicy(
                BackoffStrategy.Fixed,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMinutes(1)),
            CancellationToken.None);
    }

    private static SubmitJobCommand CreateCommand(
        string queue,
        ExecutionOrderingMode mode,
        string? concurrencyKey = null) =>
        new(
            JobKey: "test",
            PayloadJson: "{}",
            Queue: queue,
            Priority: 0,
            AvailableAt: DateTimeOffset.UtcNow,
            IdempotencyKey: null,
            ConcurrencyKey: concurrencyKey,
            MaxAttempts: 5,
            TimeoutSeconds: 300,
            DeliveryTarget: new DeliveryTarget(
                ExecutionDeliveryProfile.Pull,
                ExecutionLane: "default",
                TransportId: null,
                ConsumerGroup: "default",
                OrderingMode: mode));
}
