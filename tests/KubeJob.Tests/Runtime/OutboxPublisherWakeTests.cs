using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

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
                break;
            }
        });

        for (var i = 0; i < 100; i++)
        {
            signal.Signal();
        }

        await consumer.WaitAsync(TimeSpan.FromSeconds(1));
        wakes.Should().Be(1);
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
    public async Task Outbox_publisher_dispatches_immediately_when_wake_signal_fires()
    {
        var store = new InMemoryJobRuntimeStore();
        var notifier = new RecordingNotifier();
        var wake = new OutboxPublisherSignal();
        var publisher = CreatePublisher(store, notifier, wake, TimeSpan.FromSeconds(30));

        using var cts = new CancellationTokenSource();
        await publisher.StartAsync(cts.Token);

        await store.SubmitAsync(NewCommand("wake.job", "wake-key"), CancellationToken.None);
        wake.Signal();

        await notifier.FirstPublished.Task.WaitAsync(TimeSpan.FromSeconds(2));

        await cts.CancelAsync();
        await publisher.StopAsync(CancellationToken.None);
        notifier.Count.Should().Be(1);
    }

    [Fact]
    public async Task Outbox_publisher_falls_back_to_poll_interval_without_signal()
    {
        var store = new InMemoryJobRuntimeStore();
        var notifier = new RecordingNotifier();
        var publisher = CreatePublisher(
            store,
            notifier,
            new OutboxPublisherSignal(),
            TimeSpan.FromMilliseconds(50));

        using var cts = new CancellationTokenSource();
        await publisher.StartAsync(cts.Token);

        await store.SubmitAsync(NewCommand("fallback.job", "fallback-key"), CancellationToken.None);

        await notifier.FirstPublished.Task.WaitAsync(TimeSpan.FromSeconds(2));

        await cts.CancelAsync();
        await publisher.StopAsync(CancellationToken.None);
        notifier.Count.Should().Be(1);
    }

    private static OutboxPublisherService CreatePublisher(
        InMemoryJobRuntimeStore store,
        IWorkAvailableNotifier notifier,
        OutboxPublisherSignal wake,
        TimeSpan pollInterval) =>
        new(
            store,
            notifier,
            wake,
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = pollInterval,
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8,
                OutboxPublishConcurrency = 1
            }),
            NullLogger<OutboxPublisherService>.Instance);

    private static SubmitJobCommand NewCommand(string jobKey, string idempotencyKey) =>
        new(
            jobKey,
            "{\"k\":\"v\"}",
            "default",
            0,
            DateTimeOffset.UtcNow,
            IdempotencyKey: idempotencyKey,
            ConcurrencyKey: null,
            MaxAttempts: 1,
            TimeoutSeconds: 30,
            DeliveryTarget: new DeliveryTarget(
                ExecutionDeliveryProfile.Pull,
                "default",
                null,
                "default"));

    private sealed class RecordingNotifier : IWorkAvailableNotifier
    {
        private int _count;
        public int Count => Volatile.Read(ref _count);
        public TaskCompletionSource<WorkAvailableSignal> FirstPublished { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask PublishAsync(WorkAvailableSignal signal, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _count);
            FirstPublished.TrySetResult(signal);
            return ValueTask.CompletedTask;
        }
    }
}
