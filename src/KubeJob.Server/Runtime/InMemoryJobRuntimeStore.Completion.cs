using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            _runs.TryGetValue(request.RunId, out var run);
            _attempts.TryGetValue(request.AttemptId, out var attempt);
            var sessionValid = TryGetSession(
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                out _);
            var now = DateTimeOffset.UtcNow;

            if (!sessionValid
                || run is null
                || attempt is null
                || attempt.Phase != JobAttemptPhase.Running
                || attempt.LeaseExpiresAt <= now
                || !string.Equals(attempt.RunId, request.RunId, StringComparison.Ordinal)
                || attempt.AttemptNumber != request.AttemptNumber
                || !string.Equals(attempt.WorkerId, request.WorkerId, StringComparison.Ordinal)
                || !string.Equals(attempt.SessionId, request.SessionId, StringComparison.Ordinal)
                || attempt.SessionEpoch != request.SessionEpoch
                || !string.Equals(attempt.LeaseToken, request.LeaseToken, StringComparison.Ordinal)
                || !string.Equals(run.CurrentAttemptId, attempt.Id, StringComparison.Ordinal))
            {
                return ValueTask.FromResult(new CompleteAttemptResponse(
                    false,
                    run?.Phase ?? JobPhase.Failed,
                    false,
                    "stale_session_attempt_expired_or_fencing_token_mismatch"));
            }

            attempt.CompletedAt = now;
            attempt.FailureCode = request.FailureCode;
            attempt.FailureMessage = request.FailureMessage;
            attempt.Phase = MapAttemptPhase(request.Outcome);

            if (run.CancelRequested || request.Outcome == JobAttemptOutcome.Canceled)
            {
                MakeTerminal(run, JobPhase.Canceled, now, request.FailureCode ?? "canceled", request.FailureMessage);
                return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, false));
            }

            switch (request.Outcome)
            {
                case JobAttemptOutcome.Succeeded:
                    MakeTerminal(run, JobPhase.Succeeded, now, null, null);
                    return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, false));

                case JobAttemptOutcome.PermanentFailure:
                    MakeTerminal(run, JobPhase.Failed, now, request.FailureCode, request.FailureMessage);
                    return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, false));

                case JobAttemptOutcome.RetryableFailure:
                case JobAttemptOutcome.TimedOut:
                    if (run.AttemptCount < run.MaxAttempts)
                    {
                        Requeue(run, now.Add(retryPolicy.ComputeDelay(run.AttemptCount)), request.FailureCode, request.FailureMessage);
                        AddWorkAvailableOutbox(run, now);
                        return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, true));
                    }

                    MakeTerminal(run, JobPhase.Dead, now, request.FailureCode, request.FailureMessage);
                    return ValueTask.FromResult(new CompleteAttemptResponse(true, run.Phase, false));

                default:
                    throw new ArgumentOutOfRangeException(nameof(request.Outcome), request.Outcome, null);
            }
        }
    }

    public ValueTask<IReadOnlyList<CompleteAttemptResponse>> CompleteBatchAsync(
        IReadOnlyList<CompleteAttemptRequest> requests,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(requests);
        cancellationToken.ThrowIfCancellationRequested();
        return CompleteBatchCoreAsync(requests, retryPolicy, cancellationToken);
    }

    private async ValueTask<IReadOnlyList<CompleteAttemptResponse>> CompleteBatchCoreAsync(
        IReadOnlyList<CompleteAttemptRequest> requests,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken)
    {
        var results = new CompleteAttemptResponse[requests.Count];
        for (var index = 0; index < requests.Count; index++)
        {
            results[index] = await CompleteAsync(requests[index], retryPolicy, cancellationToken);
        }

        return results;
    }

    public ValueTask<int> RequeueExpiredLeasesAsync(
        DateTimeOffset now,
        RetryPolicy retryPolicy,
        int batchSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var expired = _attempts.Values
                .Where(x => x.Phase == JobAttemptPhase.Running && x.LeaseExpiresAt <= now)
                .OrderBy(x => x.LeaseExpiresAt)
                .Take(Math.Max(0, batchSize))
                .ToArray();

            var changed = 0;
            foreach (var attempt in expired)
            {
                if (!_runs.TryGetValue(attempt.RunId, out var run)
                    || !string.Equals(run.CurrentAttemptId, attempt.Id, StringComparison.Ordinal))
                {
                    continue;
                }

                attempt.Phase = JobAttemptPhase.LeaseLost;
                attempt.CompletedAt = now;
                attempt.FailureCode = "lease_lost";
                attempt.FailureMessage = "The worker did not renew the attempt lease before it expired.";

                if (run.CancelRequested)
                {
                    MakeTerminal(run, JobPhase.Canceled, now, "canceled", run.FailureMessage);
                }
                else if (run.AttemptCount < run.MaxAttempts)
                {
                    Requeue(run, now.Add(retryPolicy.ComputeDelay(run.AttemptCount)), attempt.FailureCode, attempt.FailureMessage);
                    AddWorkAvailableOutbox(run, now);
                }
                else
                {
                    MakeTerminal(run, JobPhase.Dead, now, attempt.FailureCode, attempt.FailureMessage);
                }

                changed++;
            }

            return ValueTask.FromResult(changed);
        }
    }
}
