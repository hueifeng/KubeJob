using KubeJob.Core.Events;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Runtime;

public sealed record BrokerNativeEventProcessingResult(
    BrokerNativeMessageDisposition Disposition,
    WorkerExecutionResult Execution,
    BrokerNativeEventMessage? RetryMessage = null);

/// <summary>
/// Executes one event for one Subscription through the shared worker engine.
/// Retry/DLQ are intentionally subscription-scoped by the broker adapter.
/// </summary>
public sealed class BrokerNativeEventProcessor
{
    private readonly IWorkerExecutionEngine _executionEngine;
    private readonly KubeJobWorkerOptions _worker;
    private readonly string _processSessionId = $"broker-event-{Guid.NewGuid():N}";
    private readonly string _hostName = Environment.MachineName;

    public BrokerNativeEventProcessor(
        IWorkerExecutionEngine executionEngine,
        IOptions<KubeJobWorkerOptions> worker)
    {
        _executionEngine = executionEngine;
        _worker = worker.Value;
        _worker.ValidateEventWorker();
    }

    public async ValueTask<BrokerNativeEventProcessingResult> ProcessAsync(
        BrokerNativeEventMessage message,
        EventSubscriptionDefinition subscription,
        CancellationToken attemptCancellationToken,
        CancellationToken workerStoppingToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(subscription);
        message.Validate();

        if (!string.Equals(message.Topic, subscription.Topic, StringComparison.Ordinal)
            || !string.Equals(message.RoutingKey, subscription.RoutingKey, StringComparison.Ordinal))
        {
            return PermanentFailure(
                "event_subscription_mismatch",
                $"Event '{message.Topic}/{message.RoutingKey}' does not match subscription " +
                $"'{subscription.Topic}/{subscription.RoutingKey}/{subscription.Subscription}'.");
        }

        var execution = await _executionEngine.ExecuteAsync(
            new WorkerExecutionRequest(
                message.EventId,
                $"{message.EventId}:event:{subscription.Subscription}:{message.Attempt}",
                message.Attempt,
                subscription.HandlerKey,
                message.PayloadJson,
                message.TimeoutSeconds,
                new WorkerExecutionInfo(
                    _worker.WorkerId,
                    _processSessionId,
                    SessionEpoch: 0,
                    _hostName,
                    _worker.BuildId),
                attemptCancellationToken,
                workerStoppingToken));

        workerStoppingToken.ThrowIfCancellationRequested();

        return execution.Outcome switch
        {
            JobAttemptOutcome.Succeeded => new(
                BrokerNativeMessageDisposition.Ack,
                execution),

            JobAttemptOutcome.Canceled => new(
                BrokerNativeMessageDisposition.Ack,
                execution),

            JobAttemptOutcome.PermanentFailure => new(
                BrokerNativeMessageDisposition.DeadLetter,
                execution),

            JobAttemptOutcome.RetryableFailure or JobAttemptOutcome.TimedOut
                when message.RetryPolicy?.CanRetry(message.Attempt, message.MaxAttempts)
                    ?? (message.Attempt < message.MaxAttempts) => new(
                    BrokerNativeMessageDisposition.Retry,
                    execution,
                    message with { Attempt = message.Attempt + 1 }),

            _ => new(
                BrokerNativeMessageDisposition.DeadLetter,
                execution)
        };
    }

    private static BrokerNativeEventProcessingResult PermanentFailure(
        string code,
        string description)
    {
        var execution = new WorkerExecutionResult(
            JobAttemptOutcome.PermanentFailure,
            code,
            description);
        return new BrokerNativeEventProcessingResult(
            BrokerNativeMessageDisposition.DeadLetter,
            execution);
    }
}
