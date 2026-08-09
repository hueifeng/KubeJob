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
    public async Task Persisted_completion_intent_can_be_recovered_and_is_removed_by_completion()
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send", "{}", "default", 0, DateTimeOffset.UtcNow,
                null, null, 1, 30),
            CancellationToken.None)).Run;
        var worker = await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-1", "session-1", "test", "localhost", 1,
                new[] { "default" }, new[] { "mail.send" }, new Dictionary<string, string>()),
            CancellationToken.None);
        var claim = (await store.ClaimAsync(
            new ClaimJobsRequest(
                worker.WorkerId, worker.SessionId, worker.Epoch, 1,
                new[] { "default" }, new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        var request = new CompleteAttemptRequest(
            worker.WorkerId,
            worker.SessionId,
            worker.Epoch,
            claim.RunId,
            claim.AttemptId,
            claim.AttemptNumber,
            claim.LeaseToken,
            JobAttemptOutcome.Succeeded,
            FenceVersion: claim.FenceVersion);

        (await store.PersistAsync(request, CancellationToken.None)).Should().BeTrue();
        (await store.GetPendingAsync(10, CancellationToken.None)).Should().ContainSingle()
            .Which.AttemptId.Should().Be(claim.AttemptId);

        var result = await store.CompleteAsync(request, RetryPolicy, CancellationToken.None);

        result.Accepted.Should().BeTrue();
        (await store.GetRunAsync(run.Id, CancellationToken.None))!.Phase.Should().Be(JobPhase.Succeeded);
        (await store.GetPendingAsync(10, CancellationToken.None)).Should().BeEmpty();
    }

    [Fact]
    public async Task Completion_intent_is_idempotent_only_for_the_same_fence()
    {
        var store = new InMemoryJobRuntimeStore();
        var run = (await store.SubmitAsync(
            new SubmitJobCommand("mail.send", "{}", "default", 0, DateTimeOffset.UtcNow,
                null, null, 1, 30), CancellationToken.None)).Run;
        var worker = await store.RegisterAsync(new RegisterWorkerSessionRequest(
            "worker-1", "session-1", "test", "localhost", 1,
            new[] { "default" }, new[] { "mail.send" }, new Dictionary<string, string>()), CancellationToken.None);
        var claim = (await store.ClaimAsync(new ClaimJobsRequest(
            worker.WorkerId, worker.SessionId, worker.Epoch, 1,
            new[] { "default" }, new[] { "mail.send" }),
            TimeSpan.FromMinutes(1), 1, CancellationToken.None)).Single();
        var request = new CompleteAttemptRequest(
            worker.WorkerId, worker.SessionId, worker.Epoch, run.Id, claim.AttemptId,
            claim.AttemptNumber, claim.LeaseToken, JobAttemptOutcome.Succeeded,
            FenceVersion: claim.FenceVersion);

        (await store.PersistAsync(request, CancellationToken.None)).Should().BeTrue();
        (await store.PersistAsync(request with { FenceVersion = claim.FenceVersion + 1 }, CancellationToken.None))
            .Should().BeFalse();

        await store.RemoveAsync(claim.AttemptId, CancellationToken.None);
        (await store.GetPendingAsync(10, CancellationToken.None)).Should().BeEmpty();
    }
}
