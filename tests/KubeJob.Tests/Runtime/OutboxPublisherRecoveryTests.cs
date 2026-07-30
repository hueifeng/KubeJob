using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Verifies the per-message outbox dispatch contract. Previously the publisher
/// aggregated publications into a single <c>MarkPublishedAsync</c> call; a
/// failure between dispatch and the final commit would leave already-published
/// rows stuck in the Publishing state and cause duplicate broker dispatches on
/// the next poll. Each message is now durably committed before the next is
/// dispatched, so partial-batch failures are isolated.
/// </summary>
public sealed class OutboxPublisherRecoveryTests
{
    [Fact]
    public async Task Each_message_is_durably_published_before_the_next_is_dispatched()
    {
        var store = new InMemoryJobRuntimeStore();
        for (var index = 0; index < 3; index++)
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
                OutboxPollInterval = TimeSpan.FromMilliseconds(10),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8
            }),
            NullLogger<OutboxPublisherService>.Instance);

        using var cancellation = new CancellationTokenSource();
        await publisher.StartAsync(cancellation.Token);

        // Wait until all three envelopes have been dispatched.
        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (transport.Count < 3 && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(10, cancellation.Token);
        }

        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        transport.Count.Should().Be(3);

        // Inspect the underlying store: every outbox row should be in
        // Published state, never stranded in Publishing.
        var overview = await store.GetOverviewAsync(10, CancellationToken.None);
        overview.PendingOutboxMessages.Should().Be(0);
    }

    [Fact]
    public async Task Failure_on_one_message_does_not_revert_already_published_messages()
    {
        var store = new InMemoryJobRuntimeStore();
        for (var index = 0; index < 4; index++)
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
                    DeliveryTarget: BrokerTarget),
                CancellationToken.None);
        }

        var transport = new ThrowingTransport(failOnDispatchIndex: 2);
        var publisher = new OutboxPublisherService(
            store,
            new NullNotifier(),
            new ExecutionTransportRegistry(new[] { (IExecutionTransport)transport }),
            new NoopCancelPublisher(),
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(10),
                OutboxClaimDuration = TimeSpan.FromSeconds(30),
                OutboxBatchSize = 8,
                OutboxFailureDelay = TimeSpan.FromMilliseconds(50)
            }),
            NullLogger<OutboxPublisherService>.Instance);

        using var cancellation = new CancellationTokenSource();
        await publisher.StartAsync(cancellation.Token);

        // Wait until two envelopes have been dispatched before the failure,
        // then the failed one retries, then a third and fourth succeed.
        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (transport.SuccessfulDispatchCount < 3 && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(10, cancellation.Token);
        }

        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        transport.SuccessfulDispatchCount.Should().BeGreaterThanOrEqualTo(3);
        transport.FailedDispatchCount.Should().BeGreaterThanOrEqualTo(1);
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

    private sealed class ThrowingTransport : IExecutionTransport
    {
        private readonly int _failOnDispatchIndex;
        private int _successes;
        private int _failures;
        private int _totalSeen;

        public ThrowingTransport(int failOnDispatchIndex)
        {
            _failOnDispatchIndex = failOnDispatchIndex;
        }

        public string TransportId => "recording";

        public int SuccessfulDispatchCount => Volatile.Read(ref _successes);
        public int FailedDispatchCount => Volatile.Read(ref _failures);

        public ValueTask PublishAsync(
            ExecutionEnvelope envelope,
            CancellationToken cancellationToken)
        {
            var seen = Interlocked.Increment(ref _totalSeen);
            if (seen - 1 == _failOnDispatchIndex)
            {
                Interlocked.Increment(ref _failures);
                throw new InvalidOperationException("synthetic dispatch failure");
            }

            Interlocked.Increment(ref _successes);
            return ValueTask.CompletedTask;
        }
    }
}
