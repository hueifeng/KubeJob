using FluentAssertions;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Extensions;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace KubeJob.Tests.Runtime;

public sealed class WorkerExecutionEngineTests
{
    [Fact]
    public void AddKubeJobWorker_registers_transport_neutral_execution_engine()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobWorker(options =>
        {
            options.WorkerId = "worker-di";
            options.Queues = new List<string> { "default" };
        });
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IWorkerExecutionEngine>()
            .Should().BeOfType<WorkerExecutionEngine>();
    }

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

    [Fact]
    public async Task Handler_operation_canceled_without_runtime_token_is_not_misclassified_as_timeout()
    {
        var services = new ServiceCollection();
        await using var provider = services.BuildServiceProvider();
        var registry = new JobHandlerRegistry(new[]
        {
            new OperationCanceledInvoker("order.created")
        });
        var engine = new WorkerExecutionEngine(
            provider.GetRequiredService<IServiceScopeFactory>(),
            registry,
            NullLogger.Instance);

        var result = await engine.ExecuteAsync(CreateRequest("order.created"));

        result.Outcome.Should().Be(JobAttemptOutcome.RetryableFailure);
        result.FailureCode.Should().Be("handler_operation_canceled");
    }

    [Fact]
    public async Task Runtime_timeout_is_classified_as_timed_out()
    {
        var services = new ServiceCollection();
        await using var provider = services.BuildServiceProvider();
        var registry = new JobHandlerRegistry(new[]
        {
            new WaitForCancellationInvoker("order.created")
        });
        var engine = new WorkerExecutionEngine(
            provider.GetRequiredService<IServiceScopeFactory>(),
            registry,
            NullLogger.Instance);

        var result = await engine.ExecuteAsync(CreateRequest("order.created") with
        {
            TimeoutSeconds = 1
        });

        result.Outcome.Should().Be(JobAttemptOutcome.TimedOut);
        result.FailureCode.Should().Be("timeout");
    }

    [Fact]
    public async Task Worker_shutdown_is_rethrown_instead_of_persisted_as_job_outcome()
    {
        var services = new ServiceCollection();
        await using var provider = services.BuildServiceProvider();
        var registry = new JobHandlerRegistry(new[]
        {
            new WaitForCancellationInvoker("order.created")
        });
        var engine = new WorkerExecutionEngine(
            provider.GetRequiredService<IServiceScopeFactory>(),
            registry,
            NullLogger.Instance);
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();

        var act = async () => await engine.ExecuteAsync(
            CreateRequest("order.created") with
            {
                WorkerStoppingToken = stopping.Token
            });

        await act.Should().ThrowAsync<OperationCanceledException>();
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

    private sealed class OperationCanceledInvoker : IJobHandlerInvoker
    {
        public OperationCanceledInvoker(string jobKey)
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
            ValueTask.FromException(new OperationCanceledException("downstream canceled its own operation"));
    }

    private sealed class WaitForCancellationInvoker : IJobHandlerInvoker
    {
        public WaitForCancellationInvoker(string jobKey)
        {
            JobKey = jobKey;
        }

        public string JobKey { get; }

        public Type PayloadType => typeof(object);

        public async ValueTask InvokeAsync(
            IServiceProvider serviceProvider,
            string payloadJson,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }
    }
}
