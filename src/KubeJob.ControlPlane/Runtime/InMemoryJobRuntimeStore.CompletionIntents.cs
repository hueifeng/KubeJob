using KubeJob.Core.Runtime;

namespace KubeJob.ControlPlane.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<bool> PersistAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            // The AttemptId is the idempotency key. A repeated HTTP request
            // after a lost response must preserve the original completion
            // rather than overwrite its outcome.
            if (_completionIntents.TryGetValue(request.AttemptId, out var existingIntent))
            {
                return ValueTask.FromResult(
                    existingIntent.RunId == request.RunId
                    && existingIntent.WorkerId == request.WorkerId
                    && existingIntent.SessionId == request.SessionId
                    && existingIntent.SessionEpoch == request.SessionEpoch
                    && existingIntent.AttemptNumber == request.AttemptNumber
                    && existingIntent.LeaseToken == request.LeaseToken
                    && existingIntent.FenceVersion == request.FenceVersion);
            }

            if (!_runs.TryGetValue(request.RunId, out var run)
                || !_attempts.TryGetValue(request.AttemptId, out var attempt)
                || !TryGetSession(request.WorkerId, request.SessionId, request.SessionEpoch, out _)
                || attempt.Phase != JobAttemptPhase.Running
                || attempt.LeaseExpiresAt <= DateTimeOffset.UtcNow
                || !string.Equals(attempt.RunId, request.RunId, StringComparison.Ordinal)
                || attempt.AttemptNumber != request.AttemptNumber
                || !string.Equals(attempt.WorkerId, request.WorkerId, StringComparison.Ordinal)
                || !string.Equals(attempt.SessionId, request.SessionId, StringComparison.Ordinal)
                || attempt.SessionEpoch != request.SessionEpoch
                || !string.Equals(attempt.LeaseToken, request.LeaseToken, StringComparison.Ordinal)
                || attempt.FenceVersion != request.FenceVersion
                || run.FenceVersion != request.FenceVersion
                || !string.Equals(run.CurrentAttemptId, request.AttemptId, StringComparison.Ordinal))
            {
                return ValueTask.FromResult(false);
            }

            _completionIntents.Add(request.AttemptId, request);
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
            return ValueTask.FromResult<IReadOnlyList<CompleteAttemptRequest>>(
                _completionIntents.Values
                    .Where(request => _attempts.TryGetValue(request.AttemptId, out var attempt)
                        && attempt.Phase == JobAttemptPhase.Running)
                    .Take(batchSize)
                    .ToArray());
        }
    }

    private void RemoveCompletionIntent(CompleteAttemptRequest request) =>
        _completionIntents.Remove(request.AttemptId);

    public ValueTask RemoveAsync(string attemptId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(attemptId);
        lock (_gate)
        {
            _completionIntents.Remove(attemptId);
        }

        return ValueTask.CompletedTask;
    }
}
