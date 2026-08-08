using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

public sealed class OutboxPublisherRecoveryTests
{
    [Fact]
    public async Task Each_message_is_durably_published_before_the_next_is_dispatched()
    {
        var store = await CreateStoreWithMessagesAsync(3);
        var notifier = new RecordingNotifier();
        var publisher = CreatePublisher(store, notifier);

        using var cancellation = new CancellationTokenSource();
        await publisher.StartAsync(cancellation.Token);
        await notifier.WaitForCountAsync(3, TimeSpan.FromSeconds(3));
        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        notifier.Count.Should().Be(3);
        (await store.GetOverviewAsync(10, CancellationToken.None)).PendingOutboxMessages.Should().Be(0);
    }

    [Fact]
    public async Task Failure_on_one_signal_does_not_revert_already_published_messages()
    {
        var store = await CreateStoreWithMessagesAsync(4);
        var notifier = new ThrowingNotifier(failOnDispatchIndex: 2);
        var publisher = CreatePublisher(store, notifier, TimeSpan.FromMilliseconds(50));

        using var cancellation = new CancellationTokenSource();
        await publisher.StartAsync(cancellation.Token);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (notifier.SuccessfulDispatchCount < 4 && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        notifier.SuccessfulDispatchCount.Should().Be(4);
        notifier.FailedDispatchCount.Should().BeGreaterThanOrEqualTo(1);
        (await store.GetOverviewAsync(10, CancellationToken.None)).PendingOutboxMessages.Should().Be(0);
    }

    private static async Task<InMemoryJobRuntimeStore> CreateStoreWithMessagesAsync(int count)
    {
        var store = new InMemoryJobRuntimeStore();
        for (var index = 0; index < count; index++)
        {
            await store.SubmitAsync(
                new SubmitJobCommand(
                    "recovery.job",
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
        IWorkAvailableNotifier notifier,
        TimeSpan? failureDelay = null) =>
        new(
            store,
            notifier,
            new OutboxPublisherSignal(),
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(10),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8,
                OutboxFailureDelay = failureDelay ?? TimeSpan.FromSeconds(5)
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
                await Task.Delay(10);
            }
        }
    }

    private sealed class ThrowingNotifier : IWorkAvailableNotifier
    {
        private readonly int _failOnDispatchIndex;
        private int _successes;
        private int _failures;
        private int _totalSeen;

        public ThrowingNotifier(int failOnDispatchIndex)
        {
            _failOnDispatchIndex = failOnDispatchIndex;
        }

        public int SuccessfulDispatchCount => Volatile.Read(ref _successes);
        public int FailedDispatchCount => Volatile.Read(ref _failures);

        public ValueTask PublishAsync(WorkAvailableSignal signal, CancellationToken cancellationToken)
        {
            var seen = Interlocked.Increment(ref _totalSeen);
            if (seen - 1 == _failOnDispatchIndex)
            {
                Interlocked.Increment(ref _failures);
                throw new InvalidOperationException("synthetic notifier failure");
            }

            Interlocked.Increment(ref _successes);
            return ValueTask.CompletedTask;
        }
    }
}
