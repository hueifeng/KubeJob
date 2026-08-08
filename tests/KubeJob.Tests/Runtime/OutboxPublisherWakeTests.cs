using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Covers the in-process wake-up signal that lets a same-process writer notify
/// the managed outbox publisher immediately instead of waiting for a poll tick.
/// </summary>
public sealed class OutboxPublisherWakeTests
{
    [Fact]
    public async Task Signal_is_coalesced_so_many_wakes_yield_a_single_reader_pulse()
    {
        var signal = new OutboxPublisherSignal();
        var wakes = 0;
        var consumer = Task.Run(async () =>
        {
            while (await signal.Reader.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
            {
                wakes++;
                signal.Reader.TryRead(out _);
            }
        });

        for (var i = 0; i < 100; i++)
        {
            signal.Signal();
        }

        await Task.Delay(50);
        wakes.Should().Be(1, "the bounded channel should drop writes once it holds a pending signal");
        signal.Dispose();
        await consumer.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Signal_does_not_block_when_the_channel_buffer_is_full()
    {
        var signal = new OutboxPublisherSignal();
        signal.Signal();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        signal.Signal();
        sw.Stop();
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(10));
    }

    [Fact]
    public async Task Outbox_publisher_notifies_within_few_hundred_ms_when_a_wake_signal_fires()
    {
        var store = new InMemoryJobRuntimeStore();
        var notifier = new RecordingNotifier();
        var wake = new OutboxPublisherSignal();
        using var cts = new CancellationTokenSource();
        var publisher = new OutboxPublisherService(
            store,
            notifier,
            wake,
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromSeconds(30),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8,
                OutboxPublishConcurrency = 1
            }),
            NullLogger<OutboxPublisherService>.Instance);

        await publisher.StartAsync(cts.Token);
        await SubmitAsync(store, "wake-key");
        wake.Signal();

        await notifier.WaitForCountAsync(1, TimeSpan.FromSeconds(2));
        await cts.CancelAsync();
        await publisher.StopAsync(CancellationToken.None);

        notifier.Count.Should().Be(1);
    }

    [Fact]
    public async Task Outbox_publisher_falls_back_to_poll_interval_when_no_signal_fires()
    {
        var store = new InMemoryJobRuntimeStore();
        var notifier = new RecordingNotifier();
        using var cts = new CancellationTokenSource();
        var publisher = new OutboxPublisherService(
            store,
            notifier,
            new OutboxPublisherSignal(),
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(50),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8,
                OutboxPublishConcurrency = 1
            }),
            NullLogger<OutboxPublisherService>.Instance);

        await publisher.StartAsync(cts.Token);
        await SubmitAsync(store, "fallback-key");
        await notifier.WaitForCountAsync(1, TimeSpan.FromSeconds(2));

        await cts.CancelAsync();
        await publisher.StopAsync(CancellationToken.None);
        notifier.Count.Should().Be(1);
    }

    private static async Task SubmitAsync(InMemoryJobRuntimeStore store, string key)
    {
        await store.SubmitAsync(
            new SubmitJobCommand(
                "wake.job",
                "{\"k\":\"v\"}",
                "default",
                0,
                DateTimeOffset.UtcNow,
                IdempotencyKey: key,
                ConcurrencyKey: null,
                MaxAttempts: 1,
                TimeoutSeconds: 30,
                DeliveryTarget: ManagedTarget),
            CancellationToken.None);
    }

    private static readonly DeliveryTarget ManagedTarget =
        new(ExecutionDeliveryProfile.Pull, "default", null, "default");

    private sealed class RecordingNotifier : IWorkAvailableNotifier
    {
        private readonly TaskCompletionSource<bool> _firstNotification =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _count) == 1)
            {
                _firstNotification.TrySetResult(true);
            }

            return ValueTask.CompletedTask;
        }

        public async Task WaitForCountAsync(int expected, TimeSpan timeout)
        {
            if (expected <= 1)
            {
                await _firstNotification.Task.WaitAsync(timeout);
                return;
            }

            var deadline = DateTimeOffset.UtcNow + timeout;
            while (Count < expected && DateTimeOffset.UtcNow < deadline)
            {
                await Task.Delay(5);
            }

            Count.Should().BeGreaterThanOrEqualTo(expected);
        }
    }
}
