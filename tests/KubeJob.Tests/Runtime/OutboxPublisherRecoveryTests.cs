using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Verifies that the retained durable delayed/recovery outbox commits each wake
/// hint independently and retries a notifier failure without reverting earlier
/// publications.
/// </summary>
public sealed class OutboxPublisherRecoveryTests
{
    [Fact]
    public async Task Each_message_is_durably_published_before_the_next_is_notified()
    {
        var store = new InMemoryJobRuntimeStore();
        await SubmitMessagesAsync(store, 3);
        var notifier = new RecordingNotifier();

        await RunPublisherAsync(store, notifier);

        notifier.SuccessfulCount.Should().Be(3);
        var overview = await store.GetOverviewAsync(10, CancellationToken.None);
        overview.PendingOutboxMessages.Should().Be(0);
    }

    [Fact]
    public async Task Failure_on_one_message_does_not_revert_already_published_messages()
    {
        var store = new InMemoryJobRuntimeStore();
        await SubmitMessagesAsync(store, 4);
        var notifier = new ThrowingNotifier(failOnNotificationIndex: 2);

        await RunPublisherAsync(store, notifier);

        notifier.SuccessfulCount.Should().BeGreaterThanOrEqualTo(3);
        notifier.FailedCount.Should().BeGreaterThanOrEqualTo(1);
    }

    private static async Task SubmitMessagesAsync(InMemoryJobRuntimeStore store, int count)
    {
        for (var index = 0; index < count; index++)
        {
            var run = (await store.SubmitAsync(
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
                    DeliveryTarget: ManagedTarget),
                CancellationToken.None)).Run;

            (await store.RequeueWorkAvailableAsync(
                run.Id,
                DateTimeOffset.UtcNow,
                CancellationToken.None)).Should().BeTrue();
        }
    }

    private static async Task RunPublisherAsync(
        InMemoryJobRuntimeStore store,
        IWorkAvailableNotifier notifier)
    {
        using var cancellation = new CancellationTokenSource();
        var publisher = new OutboxPublisherService(
            store,
            notifier,
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(10),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8,
                OutboxFailureDelay = TimeSpan.FromMilliseconds(50)
            }),
            NullLogger<OutboxPublisherService>.Instance);

        await publisher.StartAsync(cancellation.Token);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (notifier is INotificationCounter counter
            && counter.SuccessfulCount < 3
            && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(10, cancellation.Token);
        }

        await cancellation.CancelAsync();
        await publisher.StopAsync(CancellationToken.None);
    }

    private static readonly DeliveryTarget ManagedTarget =
        new(ExecutionDeliveryProfile.Pull, "default", null, "default");

    private interface INotificationCounter
    {
        int SuccessfulCount { get; }
    }

    private sealed class RecordingNotifier : IWorkAvailableNotifier, INotificationCounter
    {
        private int _successfulCount;

        public int SuccessfulCount => Volatile.Read(ref _successfulCount);

        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _successfulCount);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingNotifier : IWorkAvailableNotifier, INotificationCounter
    {
        private readonly int _failOnNotificationIndex;
        private int _successfulCount;
        private int _failedCount;
        private int _totalSeen;

        public ThrowingNotifier(int failOnNotificationIndex)
        {
            _failOnNotificationIndex = failOnNotificationIndex;
        }

        public int SuccessfulCount => Volatile.Read(ref _successfulCount);
        public int FailedCount => Volatile.Read(ref _failedCount);

        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken)
        {
            var seen = Interlocked.Increment(ref _totalSeen) - 1;
            if (seen == _failOnNotificationIndex)
            {
                Interlocked.Increment(ref _failedCount);
                throw new InvalidOperationException("synthetic notifier failure");
            }

            Interlocked.Increment(ref _successfulCount);
            return ValueTask.CompletedTask;
        }
    }
}
