using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class WorkerSessionFencingTests
{
    private static readonly RetryPolicy TestRetryPolicy =
        new(BackoffStrategy.Fixed, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));

    [Fact]
    public async Task Old_session_cannot_complete_after_new_session_is_registered()
    {
        var store = new InMemoryJobRuntimeStore();
        await SubmitAsync(store, 1);
        var oldSession = await RegisterAsync(store, "worker-1", "session-old", 1);
        var claim = (await store.ClaimAsync(
            Claim(oldSession, 1),
            TimeSpan.FromMinutes(1),
            10,
            CancellationToken.None)).Single();

        await RegisterAsync(store, "worker-1", "session-new", 1);
        var completion = await store.CompleteAsync(
            new CompleteAttemptRequest(
                oldSession.WorkerId,
                oldSession.SessionId,
                oldSession.Epoch,
                claim.RunId,
                claim.AttemptId,
                claim.AttemptNumber,
                claim.LeaseToken,
                JobAttemptOutcome.Succeeded),
            TestRetryPolicy,
            CancellationToken.None);

        completion.Accepted.Should().BeFalse();
    }

    [Fact]
    public async Task Worker_cannot_claim_more_than_registered_capacity()
    {
        var store = new InMemoryJobRuntimeStore();
        await SubmitAsync(store, 2);
        var session = await RegisterAsync(store, "worker-1", "session-1", 1);

        var first = await store.ClaimAsync(
            Claim(session, reportedSlots: 100),
            TimeSpan.FromMinutes(1),
            100,
            CancellationToken.None);
        var second = await store.ClaimAsync(
            Claim(session, reportedSlots: 100),
            TimeSpan.FromMinutes(1),
            100,
            CancellationToken.None);

        first.Should().ContainSingle();
        second.Should().BeEmpty();
    }

    [Fact]
    public async Task Closed_session_cannot_be_reopened_with_same_session_id()
    {
        var store = new InMemoryJobRuntimeStore();
        var session = await RegisterAsync(store, "worker-1", "session-1", 1);
        await store.CloseAsync(
            session.WorkerId,
            session.SessionId,
            session.Epoch,
            CancellationToken.None);

        var action = async () => await RegisterAsync(store, "worker-1", "session-1", 1);

        await action.Should().ThrowAsync<InvalidOperationException>();
    }

    private static async Task SubmitAsync(InMemoryJobRuntimeStore store, int count)
    {
        for (var index = 0; index < count; index++)
        {
            await store.SubmitAsync(
                new SubmitJobCommand(
                    "mail.send",
                    $"{{\"index\":{index}}}",
                    "default",
                    0,
                    DateTimeOffset.UtcNow,
                    null,
                    null,
                    1,
                    300),
                CancellationToken.None);
        }
    }

    private static ValueTask<WorkerSessionRecord> RegisterAsync(
        InMemoryJobRuntimeStore store,
        string workerId,
        string sessionId,
        int maxConcurrency) => store.RegisterAsync(
        new RegisterWorkerSessionRequest(
            workerId,
            sessionId,
            "test",
            "localhost",
            maxConcurrency,
            new[] { "default" },
            new[] { "mail.send" },
            new Dictionary<string, string>()),
        CancellationToken.None);

    private static ClaimJobsRequest Claim(
        WorkerSessionRecord session,
        int reportedSlots) => new(
        session.WorkerId,
        session.SessionId,
        session.Epoch,
        reportedSlots,
        new[] { "default" },
        new[] { "mail.send" });
}
