using KubeJob.Core.Runtime;

namespace KubeJob.ControlPlane.Runtime;

public sealed partial class InMemoryJobRuntimeStore : ICompletionIntentFinalizer
{
    private readonly Dictionary<string, DateTimeOffset> _completionIntentCreatedAt =
        new(StringComparer.Ordinal);

    public ValueTask<bool> PersistAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            // AttemptId is the idempotency key. Once an intent exists it is
            // immutable: a retry must carry the exact same completion payload,
            // not merely the same lease/fence identity.
            if (_completionIntents.TryGetValue(request.AttemptId, out var existingIntent))
            {
                return ValueTask.FromResult(CompletionIntentMatches(existingIntent, request));
            }

            var now = DateTimeOffset.UtcNow;
            if (!_runs.TryGetValue(request.RunId, out var run)
                || !_attempts.TryGetValue(request.AttemptId, out var attempt)
                || !TryGetSession(request.WorkerId, request.SessionId, request.SessionEpoch, out var session)
                || session.State is not (WorkerSessionState.Ready or WorkerSessionState.Draining)
                || attempt.Phase != JobAttemptPhase.Running
                || attempt.LeaseExpiresAt <= now
                || !string.Equals(attempt.RunId, request.RunId, StringComparison.Ordinal)
                || attempt.AttemptNumber != request.AttemptNumber
                || !string.Equals(attempt.WorkerId, request.WorkerId, StringComparison.Ordinal)
                || !string.Equals(attempt.SessionId, request.SessionId, StringComparison.Ordinal)
                || attempt.SessionEpoch != request.SessionEpoch
                || !string.Equals(attempt.LeaseToken, request.LeaseToken, StringComparison.Ordinal)
                || attempt.FenceVersion != request.FenceVersion
                || run.Phase != JobPhase.Running
                || run.FenceVersion != request.FenceVersion
                || !string.Equals(run.CurrentAttemptId, request.AttemptId, StringComparison.Ordinal))
            {
                return ValueTask.FromResult(false);
            }

            _completionIntents.Add(request.AttemptId, request);
            _completionIntentCreatedAt.Add(request.AttemptId, now);
            attempt.Phase = JobAttemptPhase.Completing;
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<IReadOnlyList<CompleteAttemptRequest>> GetPendingAsync(
        int batchSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (batchSize <= 0)
        {
            return ValueTask.FromResult<IReadOnlyList<CompleteAttemptRequest>>(Array.Empty<CompleteAttemptRequest>());
        }

        lock (_gate)
        {
            // Return the durable intents themselves. FinalizeAsync owns stale
            // detection so recovery can also clean up intents whose backing Run
            // was changed by an operator or older runtime version.
            return ValueTask.FromResult<IReadOnlyList<CompleteAttemptRequest>>(
                _completionIntents.Values.Take(batchSize).ToArray());
        }
    }

    public ValueTask<CompleteAttemptResponse> FinalizeAsync(
        CompleteAttemptRequest request,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_completionIntents.TryGetValue(request.AttemptId, out var persisted)
                || !CompletionIntentMatches(persisted, request)
                || !_runs.TryGetValue(request.RunId, out var run)
                || !_attempts.TryGetValue(request.AttemptId, out var attempt)
                || attempt.Phase != JobAttemptPhase.Completing
                || run.Phase != JobPhase.Running
                || attempt.FenceVersion != request.FenceVersion
                || run.FenceVersion != request.FenceVersion
                || !string.Equals(run.CurrentAttemptId, request.AttemptId, StringComparison.Ordinal))
            {
                return ValueTask.FromResult(new CompleteAttemptResponse(
                    false,
                    run?.Phase ?? JobPhase.Failed,
                    false,
                    "stale_or_conflicting_completion_intent"));
            }

            var acceptedAt = _completionIntentCreatedAt.GetValueOrDefault(
                request.AttemptId,
                DateTimeOffset.UtcNow);
            var timedOut = acceptedAt >= attempt.StartedAt.AddSeconds(run.TimeoutSeconds);
            var effectiveOutcome = timedOut ? JobAttemptOutcome.TimedOut : persisted.Outcome;
            var effectiveFailureCode = timedOut ? "timeout" : persisted.FailureCode;
            var effectiveFailureMessage = timedOut
                ? $"Execution exceeded its {run.TimeoutSeconds} second timeout before completion was accepted."
                : persisted.FailureMessage;
            var now = DateTimeOffset.UtcNow;

            attempt.CompletedAt = now;
            attempt.FailureCode = effectiveFailureCode;
            attempt.FailureMessage = effectiveFailureMessage;
            attempt.Phase = MapAttemptPhase(effectiveOutcome);

            if (run.CancelRequested || effectiveOutcome == JobAttemptOutcome.Canceled)
            {
                MakeTerminal(
                    run,
                    JobPhase.Canceled,
                    now,
                    effectiveFailureCode ?? "canceled",
                    effectiveFailureMessage);
                RemoveCompletionIntent(request);
                return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, false));
            }

            switch (effectiveOutcome)
            {
                case JobAttemptOutcome.Succeeded:
                    MakeTerminal(run, JobPhase.Succeeded, now, null, null);
                    FireContinuation(run, effectiveOutcome, now);
                    RemoveCompletionIntent(request);
                    return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, false));

                case JobAttemptOutcome.PermanentFailure:
                    MakeTerminal(run, JobPhase.Failed, now, effectiveFailureCode, effectiveFailureMessage);
                    FireContinuation(run, effectiveOutcome, now);
                    RemoveCompletionIntent(request);
                    return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, false));

                case JobAttemptOutcome.RetryableFailure:
                case JobAttemptOutcome.TimedOut:
                    if (run.AttemptCount < run.MaxAttempts)
                    {
                        var effectivePolicy = run.RetryPolicy ?? retryPolicy;
                        Requeue(
                            run,
                            now.Add(effectivePolicy.ComputeDelay(run.AttemptCount)),
                            effectiveFailureCode,
                            effectiveFailureMessage);
                        AddWorkAvailableOutbox(run, now);
                        RemoveCompletionIntent(request);
                        return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, true));
                    }

                    MakeTerminal(run, JobPhase.Dead, now, effectiveFailureCode, effectiveFailureMessage);
                    FireContinuation(run, effectiveOutcome, now);
                    RemoveCompletionIntent(request);
                    return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, false));

                default:
                    throw new ArgumentOutOfRangeException(nameof(effectiveOutcome), effectiveOutcome, null);
            }
        }
    }

    public async ValueTask<IReadOnlyList<CompleteAttemptResponse>> FinalizeBatchAsync(
        IReadOnlyList<CompleteAttemptRequest> requests,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(requests);
        var results = new CompleteAttemptResponse[requests.Count];
        for (var index = 0; index < requests.Count; index++)
        {
            results[index] = await FinalizeAsync(requests[index], retryPolicy, cancellationToken);
        }

        return results;
    }

    private static bool CompletionIntentMatches(
        CompleteAttemptRequest persisted,
        CompleteAttemptRequest request) =>
        string.Equals(persisted.AttemptId, request.AttemptId, StringComparison.Ordinal)
        && string.Equals(persisted.RunId, request.RunId, StringComparison.Ordinal)
        && string.Equals(persisted.WorkerId, request.WorkerId, StringComparison.Ordinal)
        && string.Equals(persisted.SessionId, request.SessionId, StringComparison.Ordinal)
        && persisted.SessionEpoch == request.SessionEpoch
        && persisted.AttemptNumber == request.AttemptNumber
        && string.Equals(persisted.LeaseToken, request.LeaseToken, StringComparison.Ordinal)
        && persisted.FenceVersion == request.FenceVersion
        && persisted.Outcome == request.Outcome
        && string.Equals(persisted.FailureCode, request.FailureCode, StringComparison.Ordinal)
        && string.Equals(persisted.FailureMessage, request.FailureMessage, StringComparison.Ordinal);

    private void RemoveCompletionIntent(CompleteAttemptRequest request)
    {
        _completionIntents.Remove(request.AttemptId);
        _completionIntentCreatedAt.Remove(request.AttemptId);
    }

    public ValueTask RemoveAsync(string attemptId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(attemptId);
        lock (_gate)
        {
            _completionIntents.Remove(attemptId);
            _completionIntentCreatedAt.Remove(attemptId);
        }

        return ValueTask.CompletedTask;
    }
}
