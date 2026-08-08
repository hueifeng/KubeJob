using System.Diagnostics;
using System.Text.Json;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace KubeJob.Worker.Runtime;

/// <summary>
/// Transport-neutral input for one handler execution. Runtime coordinators
/// translate their delivery model (PostgreSQL claim, broker message, etc.)
/// into this request and remain responsible for ACK/completion semantics.
/// </summary>
public sealed record WorkerExecutionRequest(
    string RunId,
    string AttemptId,
    int AttemptNumber,
    string JobKey,
    string PayloadJson,
    int TimeoutSeconds,
    WorkerExecutionInfo Worker,
    CancellationToken AttemptCancellationToken,
    CancellationToken WorkerStoppingToken,
    int? ConsumerIndex = null);

/// <summary>
/// Normalized handler result. It intentionally contains no lease, database,
/// broker ACK, or retry-queue decisions; those belong to the runtime coordinator.
/// </summary>
public sealed record WorkerExecutionResult(
    JobAttemptOutcome Outcome,
    string? FailureCode = null,
    string? FailureMessage = null);

/// <summary>
/// Executes typed KubeJob handlers through the shared DI/middleware pipeline.
/// The engine is deliberately unaware of PostgreSQL claims, worker sessions,
/// leases, RabbitMQ deliveries, ACKs, or completion persistence.
/// </summary>
public interface IWorkerExecutionEngine
{
    ValueTask<WorkerExecutionResult> ExecuteAsync(WorkerExecutionRequest request);
}

public sealed class WorkerExecutionEngine : IWorkerExecutionEngine
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly JobHandlerRegistry _registry;
    private readonly KubeJobWorkerMetrics? _metrics;
    private readonly ILogger _logger;
    private readonly JobExecutionPipelineBuilder? _pipelineBuilder;

    public WorkerExecutionEngine(
        IServiceScopeFactory scopeFactory,
        JobHandlerRegistry registry,
        ILogger logger,
        KubeJobWorkerMetrics? metrics = null,
        JobExecutionPipelineBuilder? pipelineBuilder = null)
    {
        _scopeFactory = scopeFactory;
        _registry = registry;
        _logger = logger;
        _metrics = metrics;
        _pipelineBuilder = pipelineBuilder;
    }

    public async ValueTask<WorkerExecutionResult> ExecuteAsync(WorkerExecutionRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var handlerStartedAt = 0L;
        CancellationToken timeoutToken = default;
        try
        {
            if (!_registry.TryGet(request.JobKey, out var handler))
            {
                return new WorkerExecutionResult(
                    JobAttemptOutcome.PermanentFailure,
                    "handler_not_registered",
                    $"Worker does not contain a handler for '{request.JobKey}'.");
            }

            using var scope = _scopeFactory.CreateScope();
            using var timeoutSource = new CancellationTokenSource(TimeSpan.FromSeconds(request.TimeoutSeconds));
            timeoutToken = timeoutSource.Token;
            using var executionSource = CancellationTokenSource.CreateLinkedTokenSource(
                request.WorkerStoppingToken,
                request.AttemptCancellationToken,
                timeoutToken);

            var context = new JobExecutionContext
            {
                RunId = request.RunId,
                AttemptId = request.AttemptId,
                AttemptNumber = request.AttemptNumber,
                StartedAt = DateTimeOffset.UtcNow,
                CancellationToken = executionSource.Token,
                ServiceProvider = scope.ServiceProvider,
                Worker = request.Worker
            };

            // Keep the existing middleware contract stable while moving the
            // execution mechanics out of WorkerRuntimeService.
            context.Items["_JobKey"] = request.JobKey;

            if (request.ConsumerIndex is int consumerIndex)
            {
                _logger.LogInformation(
                    "Consumer {ConsumerIndex} executing job {RunId} attempt {AttemptNumber} ({JobKey})",
                    consumerIndex,
                    request.RunId,
                    request.AttemptNumber,
                    request.JobKey);
            }
            else
            {
                _logger.LogInformation(
                    "Executing job {RunId} attempt {AttemptNumber} ({JobKey})",
                    request.RunId,
                    request.AttemptNumber,
                    request.JobKey);
            }

            var handlerTerminal = new JobExecutionDelegate(async ctx =>
            {
                await handler.InvokeAsync(
                    ctx.ServiceProvider,
                    request.PayloadJson,
                    ctx,
                    ctx.CancellationToken);
            });

            var pipeline = _pipelineBuilder is not null
                ? _pipelineBuilder.Build(handlerTerminal)
                : handlerTerminal;

            handlerStartedAt = _metrics?.IsHandlerDurationEnabled == true
                ? Stopwatch.GetTimestamp()
                : 0L;
            await pipeline(context);

            if (context.Outcome.HasValue)
            {
                RecordHandlerDuration(handlerStartedAt, context.Outcome.Value switch
                {
                    JobAttemptOutcome.PermanentFailure => "payload_invalid",
                    JobAttemptOutcome.TimedOut => "timed_out",
                    JobAttemptOutcome.Canceled => "canceled",
                    _ => "failed"
                });

                return new WorkerExecutionResult(
                    context.Outcome.Value,
                    context.FailureCode ?? "middleware_override",
                    context.FailureMessage ?? "Outcome set by execution middleware.");
            }

            RecordHandlerDuration(handlerStartedAt, "succeeded");
            return new WorkerExecutionResult(JobAttemptOutcome.Succeeded);
        }
        catch (OperationCanceledException) when (request.AttemptCancellationToken.IsCancellationRequested)
        {
            RecordHandlerDuration(handlerStartedAt, "canceled");
            return new WorkerExecutionResult(
                JobAttemptOutcome.Canceled,
                "canceled",
                "Execution was canceled by the control plane or attempt cancellation token.");
        }
        catch (OperationCanceledException) when (request.WorkerStoppingToken.IsCancellationRequested)
        {
            // Worker shutdown/drain is not a handler outcome. Runtime
            // coordinators must preserve/recover delivery ownership instead of
            // persisting or ACKing an artificial cancellation result.
            throw;
        }
        catch (OperationCanceledException) when (timeoutToken.IsCancellationRequested)
        {
            RecordHandlerDuration(handlerStartedAt, "timed_out");
            return new WorkerExecutionResult(
                JobAttemptOutcome.TimedOut,
                "timeout",
                $"Execution exceeded its {request.TimeoutSeconds} second timeout.");
        }
        catch (OperationCanceledException ex)
        {
            // A handler may throw OperationCanceledException for its own
            // downstream token or application logic. Do not call that a
            // KubeJob timeout when none of the runtime cancellation sources
            // fired; classify it as a retryable handler failure instead.
            _logger.LogWarning(
                ex,
                "KubeJob attempt {AttemptId} threw OperationCanceledException without a runtime cancellation source",
                request.AttemptId);
            RecordHandlerDuration(handlerStartedAt, "failed");
            return new WorkerExecutionResult(
                JobAttemptOutcome.RetryableFailure,
                "handler_operation_canceled",
                ex.Message);
        }
        catch (JsonException ex)
        {
            RecordHandlerDuration(handlerStartedAt, "payload_invalid");
            return new WorkerExecutionResult(
                JobAttemptOutcome.PermanentFailure,
                "payload_invalid",
                ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KubeJob attempt {AttemptId} failed", request.AttemptId);
            RecordHandlerDuration(handlerStartedAt, "failed");
            return new WorkerExecutionResult(
                JobAttemptOutcome.RetryableFailure,
                "handler_exception",
                ex.ToString());
        }
    }

    private void RecordHandlerDuration(long startedAt, string outcome)
    {
        if (startedAt != 0)
        {
            _metrics?.HandlerCompleted(Stopwatch.GetElapsedTime(startedAt), outcome);
        }
    }
}
