using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<WorkerSessionRecord> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            var key = SessionKey(request.WorkerId, request.SessionId);
            if (_sessions.TryGetValue(key, out var sameSession))
            {
                if (sameSession.State is WorkerSessionState.Closed or WorkerSessionState.Stale)
                {
                    throw new InvalidOperationException("A closed or stale worker session cannot be reopened.");
                }

                sameSession.State = WorkerSessionState.Ready;
                sameSession.AvailableSlots = sameSession.MaxConcurrency;
                sameSession.LastHeartbeatAt = DateTimeOffset.UtcNow;
                return ValueTask.FromResult(sameSession);
            }

            foreach (var existing in _sessions.Values.Where(x =>
                         string.Equals(x.WorkerId, request.WorkerId, StringComparison.Ordinal)
                         && x.State is WorkerSessionState.Ready or WorkerSessionState.Draining))
            {
                existing.State = WorkerSessionState.Stale;
                existing.AvailableSlots = 0;
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

            _sessions[key] = session;
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

            var running = CountRunningAttempts(request.WorkerId, request.SessionId, request.SessionEpoch);
            var serverAvailable = Math.Max(0, session.MaxConcurrency - running);
            session.AvailableSlots = Math.Min(Math.Max(request.AvailableSlots, 0), serverAvailable);
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

            var registeredQueues = session.Queues.ToHashSet(StringComparer.Ordinal);
            var registeredCapabilities = session.Capabilities.ToHashSet(StringComparer.Ordinal);
            var allowedQueues = request.Queues
                .Where(registeredQueues.Contains)
                .ToHashSet(StringComparer.Ordinal);
            var allowedCapabilities = request.Capabilities
                .Where(registeredCapabilities.Contains)
                .ToHashSet(StringComparer.Ordinal);

            var running = CountRunningAttempts(request.WorkerId, request.SessionId, request.SessionEpoch);
            var serverAvailable = Math.Max(0, session.MaxConcurrency - running);
            var reportedAvailable = Math.Max(request.AvailableSlots, 0);
            var claimCount = Math.Min(
                Math.Min(reportedAvailable, serverAvailable),
                Math.Max(maxBatchSize, 0));
            if (claimCount == 0 || allowedQueues.Count == 0 || allowedCapabilities.Count == 0)
            {
                session.AvailableSlots = serverAvailable;
                return ValueTask.FromResult<IReadOnlyList<ClaimedJob>>(Array.Empty<ClaimedJob>());
            }

            var now = DateTimeOffset.UtcNow;
            var eligible = _runs.Values
                .Where(run => run.Phase == JobPhase.Pending)
                .Where(run => !run.CancelRequested && run.AttemptCount < run.MaxAttempts)
                .Where(run => run.AvailableAt <= now)
                .Where(run => allowedQueues.Contains(run.Queue))
                .Where(run => allowedCapabilities.Contains(run.JobKey))
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

                var attempt = new JobAttemptRecord
                {
                    Id = NewId(),
                    RunId = run.Id,
                    AttemptNumber = run.AttemptCount + 1,
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
                if (!_attemptIdsByRun.TryGetValue(run.Id, out var ids))
                {
                    ids = new List<string>();
                    _attemptIdsByRun.Add(run.Id, ids);
                }
                ids.Add(attempt.Id);

                run.AttemptCount = attempt.AttemptNumber;
                run.CurrentAttemptId = attempt.Id;
                run.CurrentWorkerId = request.WorkerId;
                run.CurrentSessionId = request.SessionId;
                run.StartedAt ??= now;
                run.Phase = JobPhase.Running;
                run.Version++;

                claimed.Add(new ClaimedJob(
                    run.Id,
                    attempt.Id,
                    attempt.AttemptNumber,
                    attempt.LeaseToken,
                    attempt.LeaseExpiresAt,
                    run.JobKey,
                    run.PayloadJson,
                    run.Queue,
                    run.TimeoutSeconds));
            }

            session.AvailableSlots = Math.Max(0, serverAvailable - claimed.Count);
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
            if (!TryGetSession(request.WorkerId, request.SessionId, request.SessionEpoch, out var session))
            {
                return ValueTask.FromResult<IReadOnlyList<LeaseRenewalResult>>(
                    request.Attempts.Select(x => new LeaseRenewalResult(
                        x.AttemptId,
                        false,
                        false,
                        null,
                        "stale_worker_session")).ToArray());
            }

            var now = DateTimeOffset.UtcNow;
            var results = new List<LeaseRenewalResult>(request.Attempts.Count);
            foreach (var renewal in request.Attempts)
            {
                if (!_attempts.TryGetValue(renewal.AttemptId, out var attempt)
                    || attempt.Phase != JobAttemptPhase.Running
                    || attempt.LeaseExpiresAt <= now
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
                        "attempt_expired_or_fencing_token_mismatch"));
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

    private int CountRunningAttempts(string workerId, string sessionId, long sessionEpoch) =>
        _attempts.Values.Count(attempt =>
            attempt.Phase == JobAttemptPhase.Running
            && string.Equals(attempt.WorkerId, workerId, StringComparison.Ordinal)
            && string.Equals(attempt.SessionId, sessionId, StringComparison.Ordinal)
            && attempt.SessionEpoch == sessionEpoch);
}
