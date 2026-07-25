using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class IdempotencyTests
{
    [Fact]
    public async Task Same_key_and_semantically_equal_json_returns_existing_run()
    {
        var store = new InMemoryJobRuntimeStore();
        var first = await store.SubmitAsync(
            NewCommand("{\"to\":\"a@example.com\",\"retry\":1}"),
            CancellationToken.None);
        var retry = await store.SubmitAsync(
            NewCommand("{ \"retry\" : 1, \"to\" : \"a@example.com\" }"),
            CancellationToken.None);

        retry.Existing.Should().BeTrue();
        retry.Run.Id.Should().Be(first.Run.Id);
    }

    [Fact]
    public async Task Same_key_with_different_payload_throws_conflict()
    {
        var store = new InMemoryJobRuntimeStore();
        var first = await store.SubmitAsync(
            NewCommand("{\"to\":\"a@example.com\"}"),
            CancellationToken.None);

        var action = async () => await store.SubmitAsync(
            NewCommand("{\"to\":\"b@example.com\"}"),
            CancellationToken.None);

        var exception = await action.Should().ThrowAsync<IdempotencyConflictException>();
        exception.Which.ExistingJobId.Should().Be(first.Run.Id);
        exception.Which.IdempotencyKey.Should().Be("welcome:42");
    }

    [Fact]
    public async Task Same_key_with_different_job_key_throws_conflict()
    {
        var store = new InMemoryJobRuntimeStore();
        await store.SubmitAsync(
            NewCommand("{\"id\":42}"),
            CancellationToken.None);

        var command = NewCommand("{\"id\":42}") with { JobKey = "mail.cancel" };
        var action = async () => await store.SubmitAsync(command, CancellationToken.None);

        await action.Should().ThrowAsync<IdempotencyConflictException>();
    }

    private static SubmitJobCommand NewCommand(string payloadJson) => new(
        "mail.send",
        payloadJson,
        "default",
        0,
        DateTimeOffset.UtcNow,
        "welcome:42",
        null,
        1,
        300);
}
