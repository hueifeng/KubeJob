using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

public sealed class OutboxPublisherConcurrencyTests
{
    [Fact]
    public async Task Publisher_drains_all_messages_with_a_single_worker_in_one_pass()
    {
        var store = await CreateStoreWithMessagesAsync(32);
        var notifier = new RecordingNotifier();
        var publisher = CreatePublisher(store, notifier, concurrency: 1);

        using var cancellation = new CancellationTokenSource();
        await publisher.StartAsync(cancellation.Token);
        await notifier.WaitForCountAsync(32, TimeSpan.FromSeconds(3));
        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        notifier.Count.Should().Be(32);
        (await store.GetOverviewAsync(10, CancellationToken.None)).PendingOutboxMessages.Should().Be(0);
    }

    [Fact]
    public async Task Multiple_workers_can_dispatch_managed_wake_signals_concurrently()
    {
        var store = await CreateStoreWithMessagesAsync(32);
        var notifier = new RecordingNotifier();
        var publisher = CreatePublisher(store, notifier, concurrency: 4);

        using var cancellation = new CancellationTokenSource();
        await publisher.StartAsync(cancellation.Token);
        await notifier.WaitForCountAsync(32, TimeSpan.FromSeconds(3));
        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        notifier.Count.Should().Be(32);
    }

    [Fact]
    public async Task DispatchOnceAsync_batches_mark_published_while_keeping_per_message_failures_and_abandons()
    {
        var store = await CreateStoreWithMessagesAsync(9);
        var claimed = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            batchSize: 9,
            CancellationToken.None);
        claimed.Should().HaveCount(9);

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
            batchSize: 9,
            dispatch: (message, _) =>
            {
                if (message.Id == abandonId)
                {
                    throw new PermanentOutboxException("bad payload");
                }

                if (message.Id == failId)
                {
                    throw new InvalidOperationException("transient notifier error");
                }

                return ValueTask.CompletedTask;
            },
            CancellationToken.None);

        batch.DispatchedIds.Should().HaveCount(7);
        batch.FailedIds.Should().ContainSingle().Which.Should().Be(failId);
        batch.Abandoned.Should().ContainSingle().Which.Should().Be(abandonId);
        (await store.GetOverviewAsync(10, CancellationToken.None)).PendingOutboxMessages.Should().Be(1);
    }

    private static async Task<InMemoryJobRuntimeStore> CreateStoreWithMessagesAsync(int count)
    {
        var store = new InMemoryJobRuntimeStore();
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
                    DeliveryTarget: new DeliveryTarget(
                        ExecutionDeliveryProfile.Pull,
                        "default",
                        null,
                        "default")),
                CancellationToken.None);
        }

        return store;
    }

    private static OutboxPublisherService CreatePublisher(
        InMemoryJobRuntimeStore store,
        RecordingNotifier notifier,
        int concurrency) =>
        new(
            store,
            notifier,
            new OutboxPublisherSignal(),
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(5),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 32,
                OutboxPublishConcurrency = concurrency
            }),
            NullLogger<OutboxPublisherService>.Instance);

    private sealed class RecordingNotifier : IWorkAvailableNotifier
    {
        private int _count;
        public int Count => Volatile.Read(ref _count);

        public ValueTask PublishAsync(WorkAvailableSignal signal, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _count);
            return ValueTask.CompletedTask;
        }

        public async Task WaitForCountAsync(int expected, TimeSpan timeout)
        {
            var deadline = DateTimeOffset.UtcNow.Add(timeout);
            while (Count < expected && DateTimeOffset.UtcNow < deadline)
            {
                await Task.Delay(5);
            }
        }
    }
}
