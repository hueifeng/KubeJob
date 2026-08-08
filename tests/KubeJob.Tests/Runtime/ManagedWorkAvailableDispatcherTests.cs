using FluentAssertions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Logging.Abstractions;

namespace KubeJob.Tests.Runtime;

public sealed class ManagedWorkAvailableDispatcherTests
{
    [Fact]
    public async Task Submission_burst_is_coalesced_by_logical_queue()
    {
        var notifier = new RecordingNotifier();
        var dispatcher = new ManagedWorkAvailableDispatcher(
            notifier,
            NullLogger<ManagedWorkAvailableDispatcher>.Instance);

        for (var index = 0; index < 100; index++)
        {
            dispatcher.Signal(NewRun($"run-{index}", "orders"));
        }

        await dispatcher.StartAsync(CancellationToken.None);
        await notifier.FirstPublish.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.StopAsync(CancellationToken.None);

        notifier.Signals.Should().ContainSingle();
        notifier.Signals[0].Queue.Should().Be("orders");
        notifier.Signals[0].RunId.Should().Be("run-99");
    }

    [Fact]
    public async Task Different_queues_are_signalled_independently()
    {
        var notifier = new RecordingNotifier(expectedCount: 2);
        var dispatcher = new ManagedWorkAvailableDispatcher(
            notifier,
            NullLogger<ManagedWorkAvailableDispatcher>.Instance);

        dispatcher.Signal(NewRun("orders-run", "orders"));
        dispatcher.Signal(NewRun("billing-run", "billing"));

        await dispatcher.StartAsync(CancellationToken.None);
        await notifier.AllPublished.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.StopAsync(CancellationToken.None);

        notifier.Signals.Select(signal => signal.Queue)
            .Should().BeEquivalentTo(new[] { "orders", "billing" });
    }

    [Fact]
    public async Task Future_run_does_not_emit_immediate_wake()
    {
        var notifier = new RecordingNotifier();
        var dispatcher = new ManagedWorkAvailableDispatcher(
            notifier,
            NullLogger<ManagedWorkAvailableDispatcher>.Instance);

        dispatcher.Signal(NewRun(
            "future-run",
            "orders",
            DateTimeOffset.UtcNow.AddMinutes(5)));

        await dispatcher.StartAsync(CancellationToken.None);
        await Task.Delay(50);
        await dispatcher.StopAsync(CancellationToken.None);

        notifier.Signals.Should().BeEmpty();
    }

    private static JobRunRecord NewRun(
        string runId,
        string queue,
        DateTimeOffset? availableAt = null) => new()
    {
        Id = runId,
        JobKey = "test.job",
        PayloadJson = "{}",
        Queue = queue,
        ExecutionLane = "default",
        ConsumerGroup = "default",
        AvailableAt = availableAt ?? DateTimeOffset.UtcNow.AddSeconds(-1),
        CreatedAt = DateTimeOffset.UtcNow,
        Phase = JobPhase.Pending
    };

    private sealed class RecordingNotifier : IWorkAvailableNotifier
    {
        private readonly int _expectedCount;
        private readonly object _gate = new();

        public RecordingNotifier(int expectedCount = 1)
        {
            _expectedCount = expectedCount;
        }

        public List<WorkAvailableSignal> Signals { get; } = new();

        public TaskCompletionSource FirstPublish { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource AllPublished { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                Signals.Add(signal);
                FirstPublish.TrySetResult();
                if (Signals.Count >= _expectedCount)
                {
                    AllPublished.TrySetResult();
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
