using System.Threading.Channels;
using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Covers the in-process wake-up signal that lets a same-process writer notify
/// the outbox publisher immediately, instead of waiting for the next poll tick.
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

        // Give the reader a beat to observe.
        await Task.Delay(50);

        wakes.Should().Be(1, "the bounded channel should drop writes once it holds a pending signal");
    }

    [Fact]
    public void Signal_does_not_block_when_the_channel_buffer_is_full()
    {
        var signal = new OutboxPublisherSignal();
        signal.Signal();
        // Second call must return immediately even though the buffer is full
        // and the reader is not draining yet — TryWrite + DropWrite is non-blocking.
        var sw = System.Diagnostics.Stopwatch.StartNew();
        signal.Signal();
        sw.Stop();
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(10));
    }

    [Fact]
    public async Task Outbox_publisher_dispatches_within_few_hundred_ms_when_a_wake_signal_fires_even_if_poll_interval_is_long()
    {
        var store = new InMemoryJobRuntimeStore();
        var transport = new RecordingTransport();
        var wake = new OutboxPublisherSignal();
        var publisher = new OutboxPublisherService(
            store,
            new NullNotifier(),
            new ExecutionTransportRegistry(new[] { transport }),
            new NoopCancelPublisher(),
            wake,
            Options.Create(new JobRuntimeOptions
            {
                // Long poll interval so the test can ONLY succeed via the
                // wake signal — the row must be picked up before the next
                // poll tick fires.
                OutboxPollInterval = TimeSpan.FromSeconds(30),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8,
                OutboxPublishConcurrency = 1
            }),
            NullLogger<OutboxPublisherService>.Instance);

        using var cts = new CancellationTokenSource();
        await publisher.StartAsync(cts.Token);

        // The row must land after StartAsync: the publisher's startup scan
        // would otherwise dispatch it regardless of the wake mechanism, which
        // would make this test pass vacuously. With the row submitted while
        // the publisher idles in its 30s poll wait, only the wake signal can
        // trigger dispatch.
        await store.SubmitAsync(
            new SubmitJobCommand(
                "wake.job",
                "{\"k\":\"v\"}",
                "default",
                0,
                DateTimeOffset.UtcNow,
                IdempotencyKey: "wake-key",
                ConcurrencyKey: null,
                MaxAttempts: 1,
                TimeoutSeconds: 30,
                DeliveryTarget: BrokerTarget),
            CancellationToken.None);

        // Signal a wake; the publisher should drain the row immediately.
        wake.Signal();

        var deadline = DateTimeOffset.UtcNow.AddSeconds(2);
        while (transport.Count < 1 && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        await cts.CancelAsync();
        await publisher.StopAsync(CancellationToken.None);

        transport.Count.Should().Be(1, "the wake signal should trigger dispatch well before the 30s poll interval");
    }

    [Fact]
    public async Task Outbox_publisher_falls_back_to_poll_interval_when_no_signal_fires()
    {
        var store = new InMemoryJobRuntimeStore();
        var transport = new RecordingTransport();
        var publisher = new OutboxPublisherService(
            store,
            new NullNotifier(),
            new ExecutionTransportRegistry(new[] { transport }),
            new NoopCancelPublisher(),
            new OutboxPublisherSignal(), // never signaled — exercises the poll path
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(50),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8,
                OutboxPublishConcurrency = 1
            }),
            NullLogger<OutboxPublisherService>.Instance);

        using var cts = new CancellationTokenSource();
        await publisher.StartAsync(cts.Token);

        // The row must land after StartAsync so the publisher's startup scan
        // cannot dispatch it; only the poll interval may pick it up.
        await store.SubmitAsync(
            new SubmitJobCommand(
                "fallback.job",
                "{\"k\":\"v\"}",
                "default",
                0,
                DateTimeOffset.UtcNow,
                IdempotencyKey: "fallback-key",
                ConcurrencyKey: null,
                MaxAttempts: 1,
                TimeoutSeconds: 30,
                DeliveryTarget: BrokerTarget),
            CancellationToken.None);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(2);
        while (transport.Count < 1 && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        await cts.CancelAsync();
        await publisher.StopAsync(CancellationToken.None);

        transport.Count.Should().Be(1, "without a wake signal the poll-interval path still dispatches the row");
    }

    private sealed class NullNotifier : IWorkAvailableNotifier
    {
        public ValueTask PublishAsync(WorkAvailableSignal signal, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }

    private sealed class NoopCancelPublisher : ICancelPublisher
    {
        public ValueTask PublishAsync(string group, string runId, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }

    private sealed class RecordingTransport : IExecutionTransport
    {
        private int _count;
        public int Count => Volatile.Read(ref _count);
        public string TransportId => "test-recording";
        public ValueTask PublishAsync(ExecutionEnvelope envelope, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _count);
            return ValueTask.CompletedTask;
        }
    }

    private static DeliveryTarget BrokerTarget => new(
        Profile: ExecutionDeliveryProfile.BrokerDispatch,
        ExecutionLane: "default",
        TransportId: "test-recording",
        ConsumerGroup: "default");
}