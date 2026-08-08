using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Telemetry;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Runtime;

public enum BrokerNativeMessageDisposition
{
    Ack = 0,
    Retry = 1,
    DeadLetter = 2
}

/// <summary>
/// Result returned to a broker adapter. The worker engine classifies handler
/// execution; the broker adapter owns the physical ACK/retry/DLQ operation.
/// </summary>
public sealed record BrokerNativeProcessingResult(
    BrokerNativeMessageDisposition Disposition,
    WorkerExecutionResult Execution,
    BrokerNativeJobMessage? RetryMessage = null);

/// <summary>
/// Converts a self-contained BrokerNative message into the common worker
/// execution pipeline. This type deliberately has no IWorkerRuntimeClient,
/// control-plane, lease, or database dependency.
/// </summary>
public sealed class BrokerNativeJobProcessor
{
    private readonly IWorkerExecutionEngine _executionEngine;
    private readonly KubeJobWorkerOptions _worker;
    private readonly KubeJobWorkerMetrics? _metrics;
    private readonly string _processSessionId = $"broker-{Guid.NewGuid():N}";
    private readonly string _hostName = Environment.MachineName;

    public BrokerNativeJobProcessor(
        IWorkerExecutionEngine executionEngine,
        IOptions<KubeJobWorkerOptions> worker,
        KubeJobWorkerMetrics? metrics = null)
    {
        _executionEngine = executionEngine;
        _worker = worker.Value;
        _worker.Validate();
        _metrics = metrics;
    }

    public async ValueTask<BrokerNativeProcessingResult> ProcessAsync(
        BrokerNativeJobMessage message,
        CancellationToken attemptCancellationToken,
        CancellationToken workerStoppingToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        message.Validate();

        if (!_worker.Queues.Contains(message.Queue, StringComparer.Ordinal))
        {
            return PermanentFailure(
                message,
                "worker_not_configured_for_queue",
                $"Worker is not configured for queue '{message.Queue}'.");
        }

        _metrics?.AttemptStarted(WorkerExecutionKind.BrokerNative);
        try
        {
            var execution = await _executionEngine.ExecuteAsync(
                new WorkerExecutionRequest(
                    message.MessageId,
                    $"{message.MessageId}:{message.Attempt}",
                    message.Attempt,
                    message.JobKey,
                    message.PayloadJson,
                    message.TimeoutSeconds,
                    new WorkerExecutionInfo(
                        _worker.WorkerId,
                        _processSessionId,
                        SessionEpoch: 0,
                        _hostName,
                        _worker.BuildId),
                    attemptCancellationToken,
                    workerStoppingToken,
                    ExecutionKind: WorkerExecutionKind.BrokerNative));

            // Shutdown/drain is not a job outcome. Let the broker adapter leave the
            // delivery unacked so RabbitMQ/Kafka can hand it to another worker.
            workerStoppingToken.ThrowIfCancellationRequested();

            return execution.Outcome switch
            {
                JobAttemptOutcome.Succeeded => new BrokerNativeProcessingResult(
                    BrokerNativeMessageDisposition.Ack,
                    execution),

                // An explicit active cancellation is terminal for this delivery.
                // BrokerNative cancellation is best-effort; once the handler has
                // observed it, retrying the same message would violate that intent.
                JobAttemptOutcome.Canceled => new BrokerNativeProcessingResult(
                    BrokerNativeMessageDisposition.Ack,
                    execution),

                JobAttemptOutcome.PermanentFailure => new BrokerNativeProcessingResult(
                    BrokerNativeMessageDisposition.DeadLetter,
                    execution),

                JobAttemptOutcome.RetryableFailure or JobAttemptOutcome.TimedOut
                    when message.Attempt < message.MaxAttempts =>
                    new BrokerNativeProcessingResult(
                        BrokerNativeMessageDisposition.Retry,
                        execution,
                        message with { Attempt = message.Attempt + 1 }),

                JobAttemptOutcome.RetryableFailure or JobAttemptOutcome.TimedOut =>
                    new BrokerNativeProcessingResult(
                        BrokerNativeMessageDisposition.DeadLetter,
                        execution),

                _ => new BrokerNativeProcessingResult(
                    BrokerNativeMessageDisposition.DeadLetter,
                    execution)
            };
        }
        finally
        {
            _metrics?.AttemptFinished(WorkerExecutionKind.BrokerNative);
        }
    }

    private static BrokerNativeProcessingResult PermanentFailure(
        BrokerNativeJobMessage message,
        string code,
        string description)
    {
        var execution = new WorkerExecutionResult(
            JobAttemptOutcome.PermanentFailure,
            code,
            description);
        return new BrokerNativeProcessingResult(
            BrokerNativeMessageDisposition.DeadLetter,
            execution);
    }
}
