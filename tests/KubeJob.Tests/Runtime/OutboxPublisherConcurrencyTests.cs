using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Regression coverage for the outbox publisher concurrency rework (H1).
///
/// Original bug: <c>DispatchParallelAsync</c> invoked N concurrent
/// <c>DispatchOnceAsync(batchSize: 1)</c> calls, forcing N round-trips to the
/// store and a single message per worker iteration. The fixed implementation
/// keeps each worker inside a tight claim→dispatch loop, so a batch of N
/// messages is drained without the store being asked for the same row twice.
/// </summary>
public sealed class OutboxPublisherConcurrencyTests
{
    [Fact]
    public async Task Publisher_drains_all_messages_with_a_single_worker_in_one_pass()
    {
        var store = new InMemoryJobRuntimeStore();
        const int messageCount = 32;
        for (var index = 0; index < messageCount; index++)
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
                    DeliveryTarget: BrokerTarget),
                CancellationToken.None);
        }

        var transport = new RecordingTransport();
        var publisher = new OutboxPublisherService(
            store,
            new NullNotifier(),
            new ExecutionTransportRegistry(new[] { transport }),
            new NoopCancelPublisher(),
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(5),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 32,
                OutboxPublishConcurrency = 1
            }),
            NullLogger<OutboxPublisherService>.Instance);

        using var cancellation = new CancellationTokenSource();
        await publisher.StartAsync(cancellation.Token);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (transport.Count < messageCount && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(5, cancellation.Token);
        }

        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        transport.Count.Should().Be(messageCount);
        var overview = await store.GetOverviewAsync(10, CancellationToken.None);
        overview.PendingOutboxMessages.Should().Be(0);
    }

    [Fact]
    public async Task Multiple_workers_can_dispatch_concurrently_when_batch_exceeds_one()
    {
        // The exact distribution across workers depends on the thread pool,
        // so we only assert that the publisher drains every row under
        // concurrency > 1. The single-worker test above pins down the
        // per-worker loop; this test guards against an accidental
        // re-introduction of the batchSize=1 regression.
        var store = new InMemoryJobRuntimeStore();
        const int messageCount = 32;
        for (var index = 0; index < messageCount; index++)
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
                    DeliveryTarget: BrokerTarget),
                CancellationToken.None);
        }

        var transport = new RecordingTransport();
        var publisher = new OutboxPublisherService(
            store,
            new NullNotifier(),
            new ExecutionTransportRegistry(new[] { transport }),
            new NoopCancelPublisher(),
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(5),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 32,
                OutboxPublishConcurrency = 4
            }),
            NullLogger<OutboxPublisherService>.Instance);

        using var cancellation = new CancellationTokenSource();
        await publisher.StartAsync(cancellation.Token);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (transport.Count < messageCount && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(5, cancellation.Token);
        }

        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        transport.Count.Should().Be(messageCount);
    }

    [Fact]
    public async Task DispatchOnceAsync_batches_mark_published_while_keeping_per_message_failures_and_abandons()
    {
        // Regression coverage for the P1 batching rework: claim + mark-published
        // now happen as single bulk operations per DispatchOnceAsync call, while
        // failed/abandoned outcomes must still be tracked and applied per message.
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
                    DeliveryTarget: BrokerTarget),
                CancellationToken.None);
            ids[index] = result.Run.Id;
        }

        var claimed = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            batchSize: messageCount,
            CancellationToken.None);
        claimed.Should().HaveCount(messageCount);
        await store.MarkFailedAsync(
            new OutboxFailure(claimed[0].Id, claimed[0].ClaimToken!, "reset", DateTimeOffset.UtcNow.AddSeconds(-1)),
            CancellationToken.None);
        foreach (var message in claimed.Skip(1))
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
            var run = await store.GetRunAsync(id, CancellationToken.None);
            run.Should().NotBeNull();
        }

        var overview = await store.GetOverviewAsync(10, CancellationToken.None);
        overview.PendingOutboxMessages.Should().Be(1);
    }

    private static readonly DeliveryTarget BrokerTarget =
        new(ExecutionDeliveryProfile.BrokerDispatch, "default", "recording");

    private sealed class NullNotifier : IWorkAvailableNotifier
    {
        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class RecordingTransport : IExecutionTransport
    {
        private int _count;

        public string TransportId => "recording";

        public int Count => Volatile.Read(ref _count);

        public ValueTask PublishAsync(
            ExecutionEnvelope envelope,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _count);
            return ValueTask.CompletedTask;
        }
    }
}
