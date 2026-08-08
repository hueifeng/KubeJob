using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Covers the compatibility durable outbox path retained for delayed/recovery
/// wake hints. Immediate submissions are covered by
/// <see cref="ManagedWorkAvailableDispatcherTests"/> instead.
/// </summary>
public sealed class OutboxPublisherPollingTests
{
    [Fact]
    public async Task Durable_recovery_wake_is_published_by_periodic_polling()
    {
        var store = new InMemoryJobRuntimeStore();
        var notifier = new RecordingNotifier();
        var run = (await store.SubmitAsync(
            new SubmitJobCommand(
                "wake.job",
                "{\"k\":\"v\"}",
                "default",
                0,
                DateTimeOffset.UtcNow,
                IdempotencyKey: "polling-recovery",
                ConcurrencyKey: null,
                MaxAttempts: 1,
                TimeoutSeconds: 30,
                DeliveryTarget: ManagedTarget),
            CancellationToken.None)).Run;
        (await store.RequeueWorkAvailableAsync(
            run.Id,
            DateTimeOffset.UtcNow,
            CancellationToken.None)).Should().BeTrue();

        using var cancellation = new CancellationTokenSource();
        var publisher = new OutboxPublisherService(
            store,
            notifier,
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(25),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8,
                OutboxPublishConcurrency = 1
            }),
            NullLogger<OutboxPublisherService>.Instance);

        await publisher.StartAsync(cancellation.Token);
        await notifier.FirstNotification.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await cancellation.CancelAsync();
        await publisher.StopAsync(CancellationToken.None);

        notifier.Count.Should().Be(1);
    }

    private static readonly DeliveryTarget ManagedTarget =
        new(ExecutionDeliveryProfile.Pull, "default", null, "default");

    private sealed class RecordingNotifier : IWorkAvailableNotifier
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public TaskCompletionSource<bool> FirstNotification { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _count) == 1)
            {
                FirstNotification.TrySetResult(true);
            }

            return ValueTask.CompletedTask;
        }
    }
}
