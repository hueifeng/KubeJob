using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Regression coverage for the managed outbox publisher. The publisher emits
/// wake-up hints; workers still claim authoritative work from the runtime store.
/// </summary>
public sealed class OutboxPublisherConcurrencyTests
{
    [Fact]
    public async Task Publisher_drains_all_messages_with_a_single_worker_in_one_pass()
    {
        var store = new InMemoryJobRuntimeStore();
        const int messageCount = 32;
        await SubmitMessagesAsync(store, messageCount);

        var notifier = new RecordingNotifier();
        await RunPublisherAsync(store, notifier, publishConcurrency: 1);

        notifier.Count.Should().Be(messageCount);
        var overview = await store.GetOverviewAsync(10, CancellationToken.None);
        overview.PendingOutboxMessages.Should().Be(0);
    }

    [Fact]
    public async Task Multiple_workers_can_publish_wake_hints_concurrently()
    {
        var store = new InMemoryJobRuntimeStore();
        const int messageCount = 32;
        await SubmitMessagesAsync(store, messageCount);

        var notifier = new RecordingNotifier();
        await RunPublisherAsync(store, notifier, publishConcurrency: 4);

        notifier.Count.Should().Be(messageCount);
    }

    [Fact]
    public async Task DispatchOnceAsync_batches_mark_published_while_keeping_per_message_failures_and_abandons()
    {
        var store = new InMemoryJobRuntimeStore();
        const int messageCount = 9;
        var ids = new string[messageCount];
        for (var index = 0; index < messageCount; index++)
        {
            var result = await store.SubmitAsync(
                new SubmitJobCommand(
                    $"job-{index}",
                    $"{{\"i\":{index}}}",
                    "default",
                    0,
                    DateTimeOffset.UtcNow,
                    IdempotencyKey: $"key-{index}",
                    ConcurrencyKey: null,
                    MaxAttempts: 1,
                    TimeoutSeconds: 30,
                    DeliveryTarget: ManagedTarget),
                CancellationToken.None);
            ids[index] = result.Run.Id;
        }

        var claimed = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            batchSize: messageCount,
            CancellationToken.None);
        claimed.Should().HaveCount(messageCount);
        foreach (var message in claimed)
        {
            await store.MarkFailedAsync(
                new OutboxFailure(message.Id, message.ClaimToken!, "reset", DateTimeOffset.UtcNow.AddSeconds(-1)),
                CancellationToken.None);
        }

        var abandonId = claimed[0].Id;
        var failId = claimed[1].Id;
        var batch = await store.DispatchOnceAsync(
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMilliseconds(50),
            batchSize: messageCount,
            dispatch: (message, _) =>
            {
                if (message.Id == abandonId)
                {
                    throw new PermanentOutboxException("bad payload");
                }

                if (message.Id == failId)
                {
                    throw new InvalidOperationException("transient transport error");
                }

                return ValueTask.CompletedTask;
            },
            CancellationToken.None);

        batch.DispatchedIds.Should().HaveCount(messageCount - 2);
        batch.FailedIds.Should().ContainSingle().Which.Should().Be(failId);
        batch.Abandoned.Should().ContainSingle().Which.Should().Be(abandonId);

        foreach (var id in ids)
        {
            (await store.GetRunAsync(id, CancellationToken.None)).Should().NotBeNull();
        }

        var overview = await store.GetOverviewAsync(10, CancellationToken.None);
        overview.PendingOutboxMessages.Should().Be(1);
    }

    private static async Task SubmitMessagesAsync(InMemoryJobRuntimeStore store, int count)
    {
        for (var index = 0; index < count; index++)
        {
            await store.SubmitAsync(
                new SubmitJobCommand(
                    $"job-{index}",
                    $"{{\"i\":{index}}}",
                    "default",
                    0,
                    DateTimeOffset.UtcNow,
                    IdempotencyKey: $"key-{index}",
                    ConcurrencyKey: null,
                    MaxAttempts: 1,
                    TimeoutSeconds: 30,
                    DeliveryTarget: ManagedTarget),
                CancellationToken.None);
        }
    }

    private static async Task RunPublisherAsync(
        InMemoryJobRuntimeStore store,
        RecordingNotifier notifier,
        int publishConcurrency)
    {
        using var cancellation = new CancellationTokenSource();
        var publisher = new OutboxPublisherService(
            store,
            notifier,
            new OutboxPublisherSignal(),
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(5),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 32,
                OutboxPublishConcurrency = publishConcurrency
            }),
            NullLogger<OutboxPublisherService>.Instance);

        await publisher.StartAsync(cancellation.Token);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (notifier.Count < 32 && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(5, cancellation.Token);
        }

        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);
    }

    private static readonly DeliveryTarget ManagedTarget =
        new(ExecutionDeliveryProfile.Pull, "default", null, "default");

    private sealed class RecordingNotifier : IWorkAvailableNotifier
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _count);
            return ValueTask.CompletedTask;
        }
    }
}
