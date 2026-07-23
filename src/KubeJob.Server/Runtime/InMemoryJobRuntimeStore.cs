using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Development and test implementation of the V2 runtime contracts.
/// All state transitions are serialized through one gate so its semantics can be
/// used as the reference model for durable providers.
/// </summary>
public sealed class InMemoryJobRuntimeStore :
    IJobSubmissionStore,
    IWorkerSessionStore,
    IJobClaimStore,
    IJobCompletionStore,
    IJobQueryStore,
    IOutboxStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, JobRunRecord> _runs = new(StringComparer.Ordinal);
    private readonly Dictionary<string, JobAttemptRecord> _attempts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<string>> _attemptIdsByRun = new(StringComparer.Ordinal);
    private readonly Dictionary<string, WorkerSessionRecord> _sessions = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _idempotency = new(StringComparer.Ordinal);
    private readonly Dictionary<string, OutboxMessageRecord> _outbox = new(StringComparer.Ordinal);

    public ValueTask<SubmitJobResult> SubmitAsync(
        SubmitJobCommand command,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!string.IsNullOrWhiteSpace(command.IdempotencyKey)
                && _idempotency.TryGetValue(command.IdempotencyKey, out var existingId)
                && _runs.TryGetValue(existingId, out var existing))
            {
                return ValueTask.FromResult(new SubmitJobResult(existing, Existing: true));
            }

            var now = DateTimeOffset.UtcNow;
            var run = new JobRunRecord
            {
                Id = NewId(),
                JobKey = command.JobKey,
                PayloadJson = command.PayloadJson,
                Queue = command.Queue,
                Priority = command.Priority,
                AvailableAt = command.AvailableAt,
                CreatedAt = now,
                IdempotencyKey = command.IdempotencyKey,
                ConcurrencyKey = command.ConcurrencyKey,
                MaxAttempts = command.MaxAttempts,
                TimeoutSeconds = command.TimeoutSeconds,
                Phase = JobPhase.Pending
            };

            _runs.Add(run.Id, run);

            if (!string.IsNullOrWhiteSpace(command.IdempotencyKey))
            {
                _idempotency.Add(command.IdempotencyKey, run.Id);
            }

            AddWorkAvailableOutbox(run, now);
            return ValueTask.FromResult(new SubmitJobResult(run, Existing: false));
        }
    }

    public ValueTask<bool> RequestCancelAsync(
        string runId,
        string? reason,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_runs.TryGetValue(runId, out var run) || IsTerminal(run.Phase))
            {
                return ValueTask.FromResult(false);
            }

            run.CancelRequested = true;
            run.FailureCode = "cancel_requested";
            run.FailureMessage = reason;
            run.Version++;

            if (run.Phase == JobPhase.Pending)
            {
                run.Phase = JobPhase.Canceled;
                run.CompletedAt = DateTimeOffset.UtcNow;
            }

            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<WorkerSessionRecord> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            foreach (var existing in _sessions.Values.Where(x =>
                         string.Equals(x.WorkerId, request.WorkerId, StringComparison.Ordinal)
                         && x.State is WorkerSessionState.Ready or WorkerSessionState.Draining))
            {
                existing.State = WorkerSessionState.Stale;
            }

            var epoch = _sessions.Values
                .Where(x => string.Equals(x.WorkerId, request.WorkerId, StringComparison.Ordinal))
                .Select(x => x.Epoch)
                .DefaultIfEmpty(0)
                .Max() + 1;

            var now = DateTimeOffset.UtcNow;
            var session = new WorkerSessionRecord
            {
                WorkerId = request.WorkerId,
                SessionId = request.SessionId,
                Epoch = epoch,
                BuildId = request.BuildId,
                HostName = request.HostName,
                MaxConcurrency = request.MaxConcurrency,
                AvailableSlots = request.MaxConcurrency,
                Queues = request.Queues.ToArray(),
                Capabilities = request.Capabilities.ToArray(),
                Labels = new Dictionary<string, string>(request.Labels, StringComparer.Ordinal),
                StartedAt = now,
                LastHeartbeatAt = now,
                State = WorkerSessionState.Ready
            };

            _sessions[SessionKey(request.WorkerId, request.SessionId)] = session;
            return ValueTask.FromResult(session);
        }
    }

    public ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!TryGetSession(request.WorkerId, request.SessionId, request.SessionEpoch, out var session))
            {
                return ValueTask.FromResult(false);
            }

            session.AvailableSlots = Math.Clamp(request.AvailableSlots, 0, session.MaxConcurrency);
            session.State = request.State;
            session.LastHeartbeatAt = DateTimeOffset.UtcNow;
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> CloseAsync(
        string workerId,
        string sessionId,
        long sessionEpoch,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!TryGetSession(workerId, sessionId, sessionEpoch, out var session))
            {
                return ValueTask.FromResult(false);
            }

            session.State = WorkerSessionState.Closed;
            session.AvailableSlots = 0;
            session.LastHeartbeatAt = DateTimeOffset.UtcNow;
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<IReadOnlyList<ClaimedJob>> ClaimAsync(
        ClaimJobsRequest request,
        TimeSpan leaseDuration,
        int maxBatchSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!TryGetSession(request.WorkerId, request.SessionId, request.SessionEpoch, out var session)
                || session.State != WorkerSessionState.Ready)
            {
                return ValueTask.FromResult<IReadOnlyList<ClaimedJob>>(Array.Empty<ClaimedJob>());
            }

            var claimCount = Math.Min(Math.Max(request.AvailableSlots, 0), Math.Max(maxBatchSize, 0));
            if (claimCount == 0 || request.Queues.Count == 0 || request.Capabilities.Count == 0)
            {
                return ValueTask.FromResult<IReadOnlyList<ClaimedJob>>(Array.Empty<ClaimedJob>());
            }

            var now = DateTimeOffset.UtcNow;
            var eligible = _runs.Values
                .Where(run => run.Phase == JobPhase.Pending)
                .Where(run => !run.CancelRequested)
                .Where(run => run.AttemptCount < run.MaxAttempts)
                .Where(run => run.AvailableAt <= now)
                .Where(run => request.Queues.Contains(run.Queue, StringComparer.Ordinal))
                .Where(run => request.Capabilities.Contains(run.JobKey, StringComparer.Ordinal))
                .OrderByDescending(run => run.Priority)
                .ThenBy(run => run.AvailableAt)
                .ThenBy(run => run.CreatedAt)
                .ThenBy(run => run.Id, StringComparer.Ordinal)
                .ToArray();

            var claimed = new List<ClaimedJob>(Math.Min(claimCount, eligible.Length));
            foreach (var run in eligible)
            {
                if (claimed.Count >= claimCount)
                {
                    break;
                }

                if (HasConcurrencyConflict(run))
                {
                    continue;
                }

                var attemptNumber = run.AttemptCount + 1;
                var attempt = new JobAttemptRecord
                {
                    Id = NewId(),
                    RunId = run.Id,
                    AttemptNumber = attemptNumber,
                    WorkerId = request.WorkerId,
                    SessionId = request.SessionId,
                    SessionEpoch = request.SessionEpoch,
                    LeaseToken = NewId(),
                    ClaimedAt = now,
                    StartedAt = now,
                    LeaseExpiresAt = now.Add(leaseDuration),
                    Phase = JobAttemptPhase.Running
                };

                _attempts.Add(attempt.Id, attempt);
                if (!_attemptIdsByRun.TryGetValue(run.Id, out var attemptIds))
                {
                    attemptIds = new List<string>();
                    _attemptIdsByRun.Add(run.Id, attemptIds);
                }
                attemptIds.Add(attempt.Id);

                run.AttemptCount = attemptNumber;
                run.CurrentAttemptId = attempt.Id;
                run.CurrentWorkerId = request.WorkerId;
                run.CurrentSessionId = request.SessionId;
                run.StartedAt ??= now;
                run.Phase = JobPhase.Running;
                run.Version++;

                claimed.Add(new ClaimedJob(
                    run.Id,
                    attempt.Id,
                    attemptNumber,
                    attempt.LeaseToken,
                    attempt.LeaseExpiresAt,
                    run.JobKey,
                    run.PayloadJson,
                    run.Queue,
                    run.TimeoutSeconds));
            }

            session.AvailableSlots = Math.Max(0, request.AvailableSlots - claimed.Count);
            session.LastHeartbeatAt = now;
            return ValueTask.FromResult<IReadOnlyList<ClaimedJob>>(claimed);
        }
    }

    public ValueTask<IReadOnlyList<LeaseRenewalResult>> RenewLeasesAsync(
        RenewLeasesRequest request,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!TryGetSession(request.WorkerId, request.SessionId, request.SessionEpoch, out var session)
                || session.State is WorkerSessionState.Closed or WorkerSessionState.Stale)
            {
                var rejected = request.Attempts
                    .Select(x => new LeaseRenewalResult(x.AttemptId, false, false, null, "stale_worker_session"))
                    .ToArray();
                return ValueTask.FromResult<IReadOnlyList<LeaseRenewalResult>>(rejected);
            }

            var now = DateTimeOffset.UtcNow;
            var results = new List<LeaseRenewalResult>(request.Attempts.Count);
            foreach (var renewal in request.Attempts)
            {
                if (!_attempts.TryGetValue(renewal.AttemptId, out var attempt)
                    || attempt.Phase != JobAttemptPhase.Running
                    || !string.Equals(attempt.WorkerId, request.WorkerId, StringComparison.Ordinal)
                    || !string.Equals(attempt.SessionId, request.SessionId, StringComparison.Ordinal)
                    || attempt.SessionEpoch != request.SessionEpoch
                    || !string.Equals(attempt.LeaseToken, renewal.LeaseToken, StringComparison.Ordinal)
                    || !_runs.TryGetValue(attempt.RunId, out var run)
                    || !string.Equals(run.CurrentAttemptId, attempt.Id, StringComparison.Ordinal))
                {
                    results.Add(new LeaseRenewalResult(
                        renewal.AttemptId,
                        false,
                        false,
                        null,
                        "attempt_or_fencing_token_mismatch"));
                    continue;
                }

                attempt.LeaseExpiresAt = now.Add(leaseDuration);
                results.Add(new LeaseRenewalResult(
                    attempt.Id,
                    true,
                    run.CancelRequested,
                    attempt.LeaseExpiresAt));
            }

            session.LastHeartbeatAt = now;
            return ValueTask.FromResult<IReadOnlyList<LeaseRenewalResult>>(results);
        }
    }

    public ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        TimeSpan retryDelay,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_attempts.TryGetValue(request.AttemptId, out var attempt)
                || !_runs.TryGetValue(request.RunId, out var run)
                || attempt.Phase != JobAttemptPhase.Running
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
                    "attempt_or_fencing_token_mismatch"));
            }

            var now = DateTimeOffset.UtcNow;
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
                        Requeue(run, now.Add(retryDelay), request.FailureCode, request.FailureMessage);
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

    public ValueTask<int> RequeueExpiredLeasesAsync(
        DateTimeOffset now,
        TimeSpan retryDelay,
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
                    Requeue(run, now.Add(retryDelay), attempt.FailureCode, attempt.FailureMessage);
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

    public ValueTask<JobRunRecord?> GetRunAsync(
        string runId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            _runs.TryGetValue(runId, out var run);
            return ValueTask.FromResult(run);
        }
    }

    public ValueTask<IReadOnlyList<JobAttemptRecord>> GetAttemptsAsync(
        string runId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_attemptIdsByRun.TryGetValue(runId, out var attemptIds))
            {
                return ValueTask.FromResult<IReadOnlyList<JobAttemptRecord>>(Array.Empty<JobAttemptRecord>());
            }

            var attempts = attemptIds.Select(id => _attempts[id]).ToArray();
            return ValueTask.FromResult<IReadOnlyList<JobAttemptRecord>>(attempts);
        }
    }

    public ValueTask<IReadOnlyList<OutboxMessageRecord>> ClaimPendingAsync(
        DateTimeOffset now,
        int batchSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var messages = _outbox.Values
                .Where(x => x.State is OutboxDeliveryState.Pending or OutboxDeliveryState.Failed)
                .Where(x => x.AvailableAt <= now)
                .OrderBy(x => x.CreatedAt)
                .Take(Math.Max(0, batchSize))
                .ToArray();

            foreach (var message in messages)
            {
                message.State = OutboxDeliveryState.Publishing;
                message.PublishAttempts++;
            }

            return ValueTask.FromResult<IReadOnlyList<OutboxMessageRecord>>(messages);
        }
    }

    public ValueTask MarkPublishedAsync(
        string messageId,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_outbox.TryGetValue(messageId, out var message))
            {
                message.State = OutboxDeliveryState.Published;
                message.PublishedAt = publishedAt;
                message.LastError = null;
            }
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask MarkFailedAsync(
        string messageId,
        string error,
        DateTimeOffset nextAttemptAt,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_outbox.TryGetValue(messageId, out var message))
            {
                message.State = OutboxDeliveryState.Failed;
                message.LastError = error;
                message.AvailableAt = nextAttemptAt;
            }
        }

        return ValueTask.CompletedTask;
    }

    private bool TryGetSession(
        string workerId,
        string sessionId,
        long sessionEpoch,
        out WorkerSessionRecord session)
    {
        return _sessions.TryGetValue(SessionKey(workerId, sessionId), out session!)
               && session.Epoch == sessionEpoch
               && session.State != WorkerSessionState.Stale;
    }

    private bool HasConcurrencyConflict(JobRunRecord candidate)
    {
        if (string.IsNullOrWhiteSpace(candidate.ConcurrencyKey))
        {
            return false;
        }

        return _runs.Values.Any(other =>
            !string.Equals(other.Id, candidate.Id, StringComparison.Ordinal)
            && other.Phase == JobPhase.Running
            && string.Equals(other.ConcurrencyKey, candidate.ConcurrencyKey, StringComparison.Ordinal));
    }

    private void AddWorkAvailableOutbox(JobRunRecord run, DateTimeOffset now)
    {
        var message = new OutboxMessageRecord
        {
            Id = NewId(),
            Queue = run.Queue,
            EventType = "work-available",
            PayloadJson = JsonSerializer.Serialize(new { runId = run.Id, queue = run.Queue }),
            AvailableAt = run.AvailableAt > now ? run.AvailableAt : now,
            CreatedAt = now,
            State = OutboxDeliveryState.Pending
        };
        _outbox.Add(message.Id, message);
    }

    private static void Requeue(
        JobRunRecord run,
        DateTimeOffset availableAt,
        string? failureCode,
        string? failureMessage)
    {
        run.Phase = JobPhase.Pending;
        run.AvailableAt = availableAt;
        run.CurrentAttemptId = null;
        run.CurrentWorkerId = null;
        run.CurrentSessionId = null;
        run.FailureCode = failureCode;
        run.FailureMessage = failureMessage;
        run.Version++;
    }

    private static void MakeTerminal(
        JobRunRecord run,
        JobPhase phase,
        DateTimeOffset completedAt,
        string? failureCode,
        string? failureMessage)
    {
        run.Phase = phase;
        run.CompletedAt = completedAt;
        run.CurrentAttemptId = null;
        run.CurrentWorkerId = null;
        run.CurrentSessionId = null;
        run.FailureCode = failureCode;
        run.FailureMessage = failureMessage;
        run.Version++;
    }

    private static JobAttemptPhase MapAttemptPhase(JobAttemptOutcome outcome) => outcome switch
    {
        JobAttemptOutcome.Succeeded => JobAttemptPhase.Succeeded,
        JobAttemptOutcome.RetryableFailure => JobAttemptPhase.RetryableFailure,
        JobAttemptOutcome.PermanentFailure => JobAttemptPhase.PermanentFailure,
        JobAttemptOutcome.Canceled => JobAttemptPhase.Canceled,
        JobAttemptOutcome.TimedOut => JobAttemptPhase.TimedOut,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, null)
    };

    private static bool IsTerminal(JobPhase phase) => phase is
        JobPhase.Succeeded or JobPhase.Failed or JobPhase.Canceled or JobPhase.Dead;

    private static string SessionKey(string workerId, string sessionId) => $"{workerId}\n{sessionId}";

    private static string NewId() => Guid.NewGuid().ToString("N");
}
