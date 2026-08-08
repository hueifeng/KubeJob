using FluentAssertions;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Extensions;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using KubeJob.Worker.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

public sealed class BrokerNativeJobProcessorTests
{
    [Fact]
    public void BrokerNative_worker_registration_does_not_register_managed_runtime_client()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobBrokerNativeWorker(options =>
        {
            options.WorkerId = "broker-worker";
            options.Queues = new List<string> { "orders" };
        });
        using var provider = services.BuildServiceProvider();

        provider.GetService<IWorkerRuntimeClient>().Should().BeNull();
        provider.GetService<WorkerRuntimeService>().Should().BeNull();
        provider.GetRequiredService<IWorkerExecutionEngine>()
            .Should().BeOfType<WorkerExecutionEngine>();
        provider.GetRequiredService<BrokerNativeJobProcessor>()
            .Should().NotBeNull();
    }

    [Fact]
    public async Task Succeeded_message_is_acked_without_control_plane_dependency()
    {
        var engine = new StubExecutionEngine(
            new WorkerExecutionResult(JobAttemptOutcome.Succeeded));
        var processor = CreateProcessor(engine);

        var result = await processor.ProcessAsync(
            CreateMessage(),
            CancellationToken.None,
            CancellationToken.None);

        result.Disposition.Should().Be(BrokerNativeMessageDisposition.Ack);
        result.RetryMessage.Should().BeNull();
        engine.Requests.Should().ContainSingle();
        engine.Requests[0].RunId.Should().Be("message-1");
        engine.Requests[0].AttemptId.Should().Be("message-1:1");
        engine.Requests[0].JobKey.Should().Be("order.created");
        engine.Requests[0].PayloadJson.Should().Be("{\"orderId\":1001}");
        engine.Requests[0].Worker.SessionEpoch.Should().Be(0);
        engine.Requests[0].Worker.SessionId.Should().StartWith("broker-");
        engine.Requests[0].ExecutionKind.Should().Be(WorkerExecutionKind.BrokerNative);
    }

    [Fact]
    public async Task Retryable_failure_increments_attempt_for_broker_republish()
    {
        var engine = new StubExecutionEngine(
            new WorkerExecutionResult(
                JobAttemptOutcome.RetryableFailure,
                "handler_exception",
                "transient"));
        var processor = CreateProcessor(engine);

        var result = await processor.ProcessAsync(
            CreateMessage() with { Attempt = 1, MaxAttempts = 3 },
            CancellationToken.None,
            CancellationToken.None);

        result.Disposition.Should().Be(BrokerNativeMessageDisposition.Retry);
        result.RetryMessage.Should().NotBeNull();
        result.RetryMessage!.Attempt.Should().Be(2);
        result.RetryMessage.MessageId.Should().Be("message-1");
    }

    [Fact]
    public async Task Retry_budget_exhaustion_dead_letters_message()
    {
        var engine = new StubExecutionEngine(
            new WorkerExecutionResult(
                JobAttemptOutcome.TimedOut,
                "timeout",
                "slow"));
        var processor = CreateProcessor(engine);

        var result = await processor.ProcessAsync(
            CreateMessage() with { Attempt = 3, MaxAttempts = 3 },
            CancellationToken.None,
            CancellationToken.None);

        result.Disposition.Should().Be(BrokerNativeMessageDisposition.DeadLetter);
        result.RetryMessage.Should().BeNull();
    }

    [Fact]
    public async Task Permanent_failure_dead_letters_without_retry()
    {
        var engine = new StubExecutionEngine(
            new WorkerExecutionResult(
                JobAttemptOutcome.PermanentFailure,
                "payload_invalid",
                "bad payload"));
        var processor = CreateProcessor(engine);

        var result = await processor.ProcessAsync(
            CreateMessage(),
            CancellationToken.None,
            CancellationToken.None);

        result.Disposition.Should().Be(BrokerNativeMessageDisposition.DeadLetter);
        result.RetryMessage.Should().BeNull();
    }

    [Fact]
    public async Task Message_for_unconfigured_queue_is_dead_lettered_before_execution()
    {
        var engine = new StubExecutionEngine(
            new WorkerExecutionResult(JobAttemptOutcome.Succeeded));
        var processor = CreateProcessor(engine);

        var result = await processor.ProcessAsync(
            CreateMessage() with { Queue = "payments" },
            CancellationToken.None,
            CancellationToken.None);

        result.Disposition.Should().Be(BrokerNativeMessageDisposition.DeadLetter);
        result.Execution.FailureCode.Should().Be("worker_not_configured_for_queue");
        engine.Requests.Should().BeEmpty();
    }

    [Fact]
    public async Task Active_cancellation_is_terminal_but_worker_shutdown_is_not_acked()
    {
        var canceledEngine = new StubExecutionEngine(
            new WorkerExecutionResult(
                JobAttemptOutcome.Canceled,
                "canceled",
                "requested"));
        var processor = CreateProcessor(canceledEngine);

        var canceled = await processor.ProcessAsync(
            CreateMessage(),
            CancellationToken.None,
            CancellationToken.None);
        canceled.Disposition.Should().Be(BrokerNativeMessageDisposition.Ack);

        using var stopping = new CancellationTokenSource();
        stopping.Cancel();
        var stoppingAction = async () => await processor.ProcessAsync(
            CreateMessage(),
            CancellationToken.None,
            stopping.Token);
        await stoppingAction.Should().ThrowAsync<OperationCanceledException>();
    }

    private static BrokerNativeJobProcessor CreateProcessor(IWorkerExecutionEngine engine) =>
        new(
            engine,
            Options.Create(new KubeJobWorkerOptions
            {
                WorkerId = "worker-1",
                BuildId = "build-1",
                Queues = new List<string> { "orders" }
            }));

    private static BrokerNativeJobMessage CreateMessage() =>
        new()
        {
            MessageId = "message-1",
            JobKey = "order.created",
            Queue = "orders",
            PayloadJson = "{\"orderId\":1001}",
            EnqueuedAt = DateTimeOffset.UtcNow,
            Attempt = 1,
            MaxAttempts = 3,
            TimeoutSeconds = 30
        };

    private sealed class StubExecutionEngine : IWorkerExecutionEngine
    {
        private readonly WorkerExecutionResult _result;

        public StubExecutionEngine(WorkerExecutionResult result)
        {
            _result = result;
        }

        public List<WorkerExecutionRequest> Requests { get; } = new();

        public ValueTask<WorkerExecutionResult> ExecuteAsync(WorkerExecutionRequest request)
        {
            Requests.Add(request);
            return ValueTask.FromResult(_result);
        }
    }
}
