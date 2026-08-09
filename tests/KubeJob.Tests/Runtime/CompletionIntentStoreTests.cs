using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class CompletionIntentStoreTests
{
    private static readonly RetryPolicy RetryPolicy =
        new(BackoffStrategy.Fixed, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));

    [Fact]
    public async Task Persisted_completion_intent_can_be_recovered_and_is_removed_by_finalization()
    {
        var (store, run, worker, claim) = await CreateClaimAsync(timeoutSeconds: 30);
        var request = Completion(worker, claim, JobAttemptOutcome.Succeeded);

        (await store.PersistAsync(request, CancellationToken.None)).Should().BeTrue();
        (await store.GetPendingAsync(10, CancellationToken.None)).Should().ContainSingle()
            .Which.AttemptId.Should().Be(claim.AttemptId);

        var result = await store.FinalizeAsync(request, RetryPolicy, CancellationToken.None);

        result.Accepted.Should().BeTrue();
        (await store.GetRunAsync(run.Id, CancellationToken.None))!.Phase.Should().Be(JobPhase.Succeeded);
        (await store.GetPendingAsync(10, CancellationToken.None)).Should().BeEmpty();
    }

    [Fact]
    public async Task Persisted_completion_intent_survives_lease_expiry_and_reaper()
    {
        var (store, run, worker, claim) = await CreateClaimAsync(
            timeoutSeconds: 30,
            leaseDuration: TimeSpan.FromMilliseconds(20));
        var request = Completion(worker, claim, JobAttemptOutcome.Succeeded);

        (await store.PersistAsync(request, CancellationToken.None)).Should().BeTrue();
        await Task.Delay(50);

        var reaped = await store.RequeueExpiredLeasesAsync(
            DateTimeOffset.UtcNow,
            RetryPolicy,
            10,
            CancellationToken.None);
        reaped.Should().Be(0);

        var result = await store.FinalizeAsync(request, RetryPolicy, CancellationToken.None);
        result.Accepted.Should().BeTrue();
        (await store.GetRunAsync(run.Id, CancellationToken.None))!.Phase.Should().Be(JobPhase.Succeeded);
    }

    [Fact]
    public async Task Completion_intent_accepted_before_timeout_is_not_reclaimed_if_finalization_is_delayed()
    {
        var (store, run, worker, claim) = await CreateClaimAsync(timeoutSeconds: 1);
        var request = Completion(worker, claim, JobAttemptOutcome.Succeeded);

        (await store.PersistAsync(request, CancellationToken.None)).Should().BeTrue();
        await Task.Delay(TimeSpan.FromMilliseconds(1100));

        var timedOut = await store.RequeueTimedOutAttemptsAsync(
            DateTimeOffset.UtcNow,
            RetryPolicy,
            10,
            CancellationToken.None);
        timedOut.Should().Be(0);

        var result = await store.FinalizeAsync(request, RetryPolicy, CancellationToken.None);
        result.Accepted.Should().BeTrue();
        (await store.GetRunAsync(run.Id, CancellationToken.None))!.Phase.Should().Be(JobPhase.Succeeded);
    }

    [Fact]
    public async Task Completion_intent_is_idempotent_only_for_the_exact_same_completion()
    {
        var (store, _, worker, claim) = await CreateClaimAsync(timeoutSeconds: 30);
        var request = Completion(worker, claim, JobAttemptOutcome.Succeeded);

        (await store.PersistAsync(request, CancellationToken.None)).Should().BeTrue();
        (await store.PersistAsync(request, CancellationToken.None)).Should().BeTrue();

        (await store.PersistAsync(
            request with { FenceVersion = claim.FenceVersion + 1 },
            CancellationToken.None)).Should().BeFalse();
        (await store.PersistAsync(
            request with { Outcome = JobAttemptOutcome.PermanentFailure, FailureCode = "conflict" },
            CancellationToken.None)).Should().BeFalse();

        await store.RemoveAsync(claim.AttemptId, CancellationToken.None);
        (await store.GetPendingAsync(10, CancellationToken.None)).Should().BeEmpty();
    }

    private static async Task<(InMemoryJobRuntimeStore Store, JobRunRecord Run, WorkerSessionRecord Worker, ClaimedJob Claim)>
        CreateClaimAsync(int timeoutSeconds, TimeSpan? leaseDuration = null)
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send",
                "{}",
                "default",
                0,
                DateTimeOffset.UtcNow,
                null,
                null,
                1,
                timeoutSeconds),
            CancellationToken.None)).Run;
        var worker = await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-1",
                "session-1",
                "test",
                "localhost",
                1,
                new[] { "default" },
                new[] { "mail.send" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        var claim = (await store.ClaimAsync(
            new ClaimJobsRequest(
                worker.WorkerId,
                worker.SessionId,
                worker.Epoch,
                1,
                new[] { "default" },
                new[] { "mail.send" }),
            leaseDuration ?? TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        return (store, run, worker, claim);
    }

    private static CompleteAttemptRequest Completion(
        WorkerSessionRecord worker,
        ClaimedJob claim,
        JobAttemptOutcome outcome) => new(
        worker.WorkerId,
        worker.SessionId,
        worker.Epoch,
        claim.RunId,
        claim.AttemptId,
        claim.AttemptNumber,
        claim.LeaseToken,
        outcome,
        FenceVersion: claim.FenceVersion);
}
