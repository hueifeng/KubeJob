using FluentAssertions;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace KubeJob.Tests.Runtime;

public sealed class WorkerExecutionEngineTests
{
    [Fact]
    public async Task ExecuteAsync_invokes_registered_handler_and_returns_success()
    {
        var services = new ServiceCollection();
        await using var provider = services.BuildServiceProvider();
        var captured = new TaskCompletionSource<JobExecutionContext>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var registry = new JobHandlerRegistry(new[]
        {
            new RecordingInvoker("order.created", captured)
        });
        var engine = new WorkerExecutionEngine(
            provider.GetRequiredService<IServiceScopeFactory>(),
            registry,
            NullLogger.Instance);

        var result = await engine.ExecuteAsync(CreateRequest("order.created"));

        result.Outcome.Should().Be(JobAttemptOutcome.Succeeded);
        result.FailureCode.Should().BeNull();
        var context = await captured.Task.WaitAsync(TimeSpan.FromSeconds(5));
        context.RunId.Should().Be("run-1");
        context.AttemptId.Should().Be("attempt-1");
        context.Items["_JobKey"].Should().Be("order.created");
        context.Worker.WorkerId.Should().Be("worker-1");
    }

    [Fact]
    public async Task ExecuteAsync_returns_permanent_failure_when_handler_is_missing()
    {
        var services = new ServiceCollection();
        await using var provider = services.BuildServiceProvider();
        var engine = new WorkerExecutionEngine(
            provider.GetRequiredService<IServiceScopeFactory>(),
            new JobHandlerRegistry(Array.Empty<IJobHandlerInvoker>()),
            NullLogger.Instance);

        var result = await engine.ExecuteAsync(CreateRequest("order.created"));

        result.Outcome.Should().Be(JobAttemptOutcome.PermanentFailure);
        result.FailureCode.Should().Be("handler_not_registered");
    }

    [Fact]
    public async Task ExecuteAsync_normalizes_handler_exception_without_runtime_client()
    {
        var services = new ServiceCollection();
        await using var provider = services.BuildServiceProvider();
        var registry = new JobHandlerRegistry(new[]
        {
            new ThrowingInvoker("order.created")
        });
        var engine = new WorkerExecutionEngine(
            provider.GetRequiredService<IServiceScopeFactory>(),
            registry,
            NullLogger.Instance);

        var result = await engine.ExecuteAsync(CreateRequest("order.created"));

        result.Outcome.Should().Be(JobAttemptOutcome.RetryableFailure);
        result.FailureCode.Should().Be("handler_exception");
        result.FailureMessage.Should().Contain("handler failed");
    }

    private static WorkerExecutionRequest CreateRequest(string jobKey) =>
        new(
            "run-1",
            "attempt-1",
            1,
            jobKey,
            "{}",
            30,
            new WorkerExecutionInfo(
                "worker-1",
                "session-1",
                1,
                "host-1",
                "build-1"),
            CancellationToken.None,
            CancellationToken.None,
            ConsumerIndex: 0);

    private sealed class RecordingInvoker : IJobHandlerInvoker
    {
        private readonly TaskCompletionSource<JobExecutionContext> _captured;

        public RecordingInvoker(
            string jobKey,
            TaskCompletionSource<JobExecutionContext> captured)
        {
            JobKey = jobKey;
            _captured = captured;
        }

        public string JobKey { get; }

        public Type PayloadType => typeof(object);

        public ValueTask InvokeAsync(
            IServiceProvider serviceProvider,
            string payloadJson,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            _captured.TrySetResult(context);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingInvoker : IJobHandlerInvoker
    {
        public ThrowingInvoker(string jobKey)
        {
            JobKey = jobKey;
        }

        public string JobKey { get; }

        public Type PayloadType => typeof(object);

        public ValueTask InvokeAsync(
            IServiceProvider serviceProvider,
            string payloadJson,
            JobExecutionContext context,
            CancellationToken cancellationToken) =>
            ValueTask.FromException(new InvalidOperationException("handler failed"));
    }
}
