using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class WorkerClaimAuthorizationTests
{
    [Fact]
    public async Task Claim_cannot_expand_registered_queues_or_capabilities()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(
            new SubmitJobCommand(
                "mail.send",
                "{}",
                "default",
                0,
                DateTimeOffset.UtcNow,
                null,
                null,
                1,
                30),
            CancellationToken.None);
        var session = await store.RegisterAsync(
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

        var wrongQueue = await store.ClaimAsync(
            new ClaimJobsRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                1,
                new[] { "admin" },
                new[] { "mail.send" }),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None);
        var wrongCapability = await store.ClaimAsync(
            new ClaimJobsRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                1,
                new[] { "default" },
                new[] { "admin.execute" }),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None);
        var registered = await store.ClaimAsync(
            new ClaimJobsRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                1,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None);

        wrongQueue.Should().BeEmpty();
        wrongCapability.Should().BeEmpty();
        registered.Should().ContainSingle();
    }
}
