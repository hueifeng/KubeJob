using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

public sealed class OutboxPublisherRoutingTests
{
    [Fact]
    public async Task Broker_route_uses_execution_dispatcher_without_changing_business_submission()
    {
        var store = new InMemoryJobRuntimeStore();
        var submission = await store.SubmitAsync(
            new SubmitJobCommand(
                "order-push-2",
                "{\"orderId\":\"O-1001\"}",
                "orders.push",
                0,
                DateTimeOffset.UtcNow,
                "order-event:1001",
                "order:O-1001",
                3,
                60),
            CancellationToken.None);
        var dispatcher = new RecordingDispatcher();
        using var cancellation = new CancellationTokenSource();
        var publisher = new OutboxPublisherService(
            store,
            new RecordingNotifier(),
            new FixedQueueRouter(new QueueRoute(
                "orders.push",
                ExecutionDeliveryProfile.BrokerDispatch)),
            dispatcher,
            Options.Create(new JobRuntimeOptions
            {
                OutboxPollInterval = TimeSpan.FromMilliseconds(10),
                OutboxClaimDuration = TimeSpan.FromSeconds(30)
            }),
            NullLogger<OutboxPublisherService>.Instance);

        await publisher.StartAsync(cancellation.Token);
        var envelope = await dispatcher.Published.Task.WaitAsync(TimeSpan.FromSeconds(2));
        cancellation.Cancel();
        await publisher.StopAsync(CancellationToken.None);

        envelope.RunId.Should().Be(submission.Run.Id);
        envelope.Queue.Should().Be("orders.push");
        envelope.EventId.Should().NotBeNullOrWhiteSpace();
    }

    private sealed class FixedQueueRouter : IQueueRouter
    {
        private readonly QueueRoute _route;

        public FixedQueueRouter(QueueRoute route)
        {
            _route = route;
        }

        public QueueRoute Resolve(string logicalQueue) =>
            _route with { Queue = logicalQueue };
    }

    private sealed class RecordingDispatcher : IExecutionDispatcher
    {
        public TaskCompletionSource<ExecutionEnvelope> Published { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask DispatchAsync(
            ExecutionEnvelope envelope,
            CancellationToken cancellationToken)
        {
            Published.TrySetResult(envelope);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingNotifier : IWorkAvailableNotifier
    {
        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
