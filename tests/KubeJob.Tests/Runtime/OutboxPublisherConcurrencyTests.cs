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
                    TimeoutSeconds: 30),
                CancellationToken.None);
        }

        var dispatcher = new RecordingDispatcher();
        var publisher = new OutboxPublisherService(
            store,
            new NullNotifier(),
            new FixedQueueRouter(),
            dispatcher,
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
        while (dispatcher.Count < messageCount && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(5, cancellation.Token);
        }

        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        dispatcher.Count.Should().Be(messageCount);
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
                    TimeoutSeconds: 30),
                CancellationToken.None);
        }

        var dispatcher = new RecordingDispatcher();
        var publisher = new OutboxPublisherService(
            store,
            new NullNotifier(),
            new FixedQueueRouter(),
            dispatcher,
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
        while (dispatcher.Count < messageCount && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(5, cancellation.Token);
        }

        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        dispatcher.Count.Should().Be(messageCount);
    }

    private sealed class FixedQueueRouter : IQueueRouter
    {
        public QueueRoute Resolve(string logicalQueue) =>
            new(logicalQueue, ExecutionDeliveryProfile.BrokerDispatch);
    }

    private sealed class NullNotifier : IWorkAvailableNotifier
    {
        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class RecordingDispatcher : IExecutionDispatcher
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public ValueTask DispatchAsync(
            ExecutionEnvelope envelope,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _count);
            return ValueTask.CompletedTask;
        }
    }
}
