using System.Text.Json;
using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<WorkerSessionRecord> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT pg_advisory_xact_lock(hashtext(@WorkerId));",
            new { request.WorkerId },
            transaction,
            cancellationToken: cancellationToken));

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET State = @Stale
            WHERE WorkerId = @WorkerId
              AND State IN (@Ready, @Draining);",
            new
            {
                request.WorkerId,
                Stale = (int)WorkerSessionState.Stale,
                Ready = (int)WorkerSessionState.Ready,
                Draining = (int)WorkerSessionState.Draining
            },
            transaction,
            cancellationToken: cancellationToken));

        var epoch = await connection.ExecuteScalarAsync<long>(new CommandDefinition(@"
            SELECT COALESCE(MAX(Epoch), 0) + 1
            FROM Kj2_WorkerSessions
            WHERE WorkerId = @WorkerId;",
            new { request.WorkerId },
            transaction,
            cancellationToken: cancellationToken));

        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));

        await connection.ExecuteAsync(new CommandDefinition(@"
            INSERT INTO Kj2_WorkerSessions
                (WorkerId, SessionId, Epoch, BuildId, HostName, State,
                 MaxConcurrency, AvailableSlots, Queues, Capabilities, Labels,
                 StartedAt, LastHeartbeatAt)
            VALUES
                (@WorkerId, @SessionId, @Epoch, @BuildId, @HostName, @State,
                 @MaxConcurrency, @AvailableSlots, CAST(@Queues AS jsonb),
                 CAST(@Capabilities AS jsonb), CAST(@Labels AS jsonb),
                 @StartedAt, @LastHeartbeatAt);",
            new
            {
                request.WorkerId,
                request.SessionId,
                Epoch = epoch,
                request.BuildId,
                request.HostName,
                State = (int)WorkerSessionState.Ready,
                request.MaxConcurrency,
                AvailableSlots = request.MaxConcurrency,
                Queues = JsonSerializer.Serialize(request.Queues, SerializerOptions),
                Capabilities = JsonSerializer.Serialize(request.Capabilities, SerializerOptions),
                Labels = JsonSerializer.Serialize(request.Labels, SerializerOptions),
                StartedAt = now,
                LastHeartbeatAt = now
            },
            transaction,
            cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return new WorkerSessionRecord
        {
            WorkerId = request.WorkerId,
            SessionId = request.SessionId,
            Epoch = epoch,
            BuildId = request.BuildId,
            HostName = request.HostName,
            State = WorkerSessionState.Ready,
            MaxConcurrency = request.MaxConcurrency,
            AvailableSlots = request.MaxConcurrency,
            Queues = request.Queues.ToArray(),
            Capabilities = request.Capabilities.ToArray(),
            Labels = new Dictionary<string, string>(request.Labels, StringComparer.Ordinal),
            StartedAt = now,
            LastHeartbeatAt = now
        };
    }

    public async ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var affected = await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET AvailableSlots = LEAST(GREATEST(@AvailableSlots, 0), MaxConcurrency),
                State = @State,
                LastHeartbeatAt = clock_timestamp()
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch
              AND State NOT IN (@Closed, @Stale);",
            new
            {
                request.AvailableSlots,
                State = (int)request.State,
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                Closed = (int)WorkerSessionState.Closed,
                Stale = (int)WorkerSessionState.Stale
            },
            cancellationToken: cancellationToken));
        return affected > 0;
    }

    public async ValueTask<bool> CloseAsync(
        string workerId,
        string sessionId,
        long sessionEpoch,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var affected = await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET State = @Closed,
                AvailableSlots = 0,
                LastHeartbeatAt = clock_timestamp()
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch
              AND State <> @Stale;",
            new
            {
                WorkerId = workerId,
                SessionId = sessionId,
                SessionEpoch = sessionEpoch,
                Closed = (int)WorkerSessionState.Closed,
                Stale = (int)WorkerSessionState.Stale
            },
            cancellationToken: cancellationToken));
        return affected > 0;
    }

    public async ValueTask<IReadOnlyList<ClaimedJob>> ClaimAsync(
        ClaimJobsRequest request,
        TimeSpan leaseDuration,
        int maxBatchSize,
        CancellationToken cancellationToken)
    {
        var limit = Math.Min(Math.Max(request.AvailableSlots, 0), Math.Max(maxBatchSize, 0));
        if (limit == 0 || request.Queues.Count == 0 || request.Capabilities.Count == 0)
        {
            return Array.Empty<ClaimedJob>();
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var accepted = await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET AvailableSlots = LEAST(GREATEST(@AvailableSlots, 0), MaxConcurrency),
                LastHeartbeatAt = clock_timestamp()
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch
              AND State = @Ready;",
            new
            {
                request.AvailableSlots,
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                Ready = (int)WorkerSessionState.Ready
            },
            transaction,
            cancellationToken: cancellationToken));

        if (accepted == 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            return Array.Empty<ClaimedJob>();
        }

        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));

        var runs = (await connection.QueryAsync<JobRunRecord>(new CommandDefinition(@"
            SELECT r.*
            FROM Kj2_JobRuns r
            WHERE r.Phase = @Pending
              AND r.CancelRequested = FALSE
              AND r.AttemptCount < r.MaxAttempts
              AND r.AvailableAt <= @Now
              AND r.Queue = ANY(@Queues)
              AND r.JobKey = ANY(@Capabilities)
              AND (
                    r.ConcurrencyKey IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM Kj2_JobRuns active
                        WHERE active.Id <> r.Id
                          AND active.Phase = @Running
                          AND active.ConcurrencyKey = r.ConcurrencyKey
                    )
                  )
            ORDER BY r.Priority DESC, r.AvailableAt, r.CreatedAt, r.Id
            FOR UPDATE SKIP LOCKED
            LIMIT @Limit;",
            new
            {
                Pending = (int)JobPhase.Pending,
                Running = (int)JobPhase.Running,
                Now = now,
                Queues = request.Queues.ToArray(),
                Capabilities = request.Capabilities.ToArray(),
                Limit = limit
            },
            transaction,
            cancellationToken: cancellationToken))).ToArray();

        var claimed = new List<ClaimedJob>(runs.Length);
        foreach (var run in runs)
        {
            var attemptNumber = run.AttemptCount + 1;
            var attemptId = NewId();
            var leaseToken = NewId();
            var leaseExpiresAt = now.Add(leaseDuration);

            await connection.ExecuteAsync(new CommandDefinition(@"
                INSERT INTO Kj2_JobAttempts
                    (Id, RunId, AttemptNumber, WorkerId, SessionId, SessionEpoch,
                     LeaseToken, Phase, ClaimedAt, StartedAt, LeaseExpiresAt)
                VALUES
                    (@Id, @RunId, @AttemptNumber, @WorkerId, @SessionId,
                     @SessionEpoch, @LeaseToken, @Phase, @ClaimedAt,
                     @StartedAt, @LeaseExpiresAt);",
                new
                {
                    Id = attemptId,
                    RunId = run.Id,
                    AttemptNumber = attemptNumber,
                    request.WorkerId,
                    request.SessionId,
                    request.SessionEpoch,
                    LeaseToken = leaseToken,
                    Phase = (int)JobAttemptPhase.Running,
                    ClaimedAt = now,
                    StartedAt = now,
                    LeaseExpiresAt = leaseExpiresAt
                },
                transaction,
                cancellationToken: cancellationToken));

            await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_JobRuns
                SET Phase = @Running,
                    AttemptCount = @AttemptNumber,
                    CurrentAttemptId = @AttemptId,
                    CurrentWorkerId = @WorkerId,
                    CurrentSessionId = @SessionId,
                    StartedAt = COALESCE(StartedAt, @StartedAt),
                    Version = Version + 1
                WHERE Id = @RunId
                  AND Phase = @Pending;",
                new
                {
                    RunId = run.Id,
                    Running = (int)JobPhase.Running,
                    Pending = (int)JobPhase.Pending,
                    AttemptNumber = attemptNumber,
                    AttemptId = attemptId,
                    request.WorkerId,
                    request.SessionId,
                    StartedAt = now
                },
                transaction,
                cancellationToken: cancellationToken));

            claimed.Add(new ClaimedJob(
                run.Id,
                attemptId,
                attemptNumber,
                leaseToken,
                leaseExpiresAt,
                run.JobKey,
                run.PayloadJson,
                run.Queue,
                run.TimeoutSeconds));
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET AvailableSlots = GREATEST(0, @ReportedSlots - @ClaimedCount),
                LastHeartbeatAt = @Now
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch;",
            new
            {
                ReportedSlots = request.AvailableSlots,
                ClaimedCount = claimed.Count,
                Now = now,
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch
            },
            transaction,
            cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return claimed;
    }

    public async ValueTask<IReadOnlyList<LeaseRenewalResult>> RenewLeasesAsync(
        RenewLeasesRequest request,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        if (request.Attempts.Count == 0)
        {
            return Array.Empty<LeaseRenewalResult>();
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var sessionValid = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(@"
            SELECT EXISTS (
                SELECT 1
                FROM Kj2_WorkerSessions
                WHERE WorkerId = @WorkerId
                  AND SessionId = @SessionId
                  AND Epoch = @SessionEpoch
                  AND State NOT IN (@Closed, @Stale)
            );",
            new
            {
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                Closed = (int)WorkerSessionState.Closed,
                Stale = (int)WorkerSessionState.Stale
            },
            transaction,
            cancellationToken: cancellationToken));

        if (!sessionValid)
        {
            await transaction.RollbackAsync(cancellationToken);
            return request.Attempts
                .Select(x => new LeaseRenewalResult(x.AttemptId, false, false, null, "stale_worker_session"))
                .ToArray();
        }

        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));
        var leaseExpiresAt = now.Add(leaseDuration);
        var results = new List<LeaseRenewalResult>(request.Attempts.Count);

        foreach (var renewal in request.Attempts)
        {
            var row = await connection.QuerySingleOrDefaultAsync<RenewalRow>(new CommandDefinition(@"
                UPDATE Kj2_JobAttempts attempt
                SET LeaseExpiresAt = @LeaseExpiresAt
                FROM Kj2_JobRuns run
                WHERE attempt.Id = @AttemptId
                  AND attempt.RunId = run.Id
                  AND attempt.Phase = @AttemptRunning
                  AND attempt.WorkerId = @WorkerId
                  AND attempt.SessionId = @SessionId
                  AND attempt.SessionEpoch = @SessionEpoch
                  AND attempt.LeaseToken = @LeaseToken
                  AND run.Phase = @RunRunning
                  AND run.CurrentAttemptId = attempt.Id
                RETURNING run.CancelRequested;",
                new
                {
                    renewal.AttemptId,
                    renewal.LeaseToken,
                    LeaseExpiresAt = leaseExpiresAt,
                    request.WorkerId,
                    request.SessionId,
                    request.SessionEpoch,
                    AttemptRunning = (int)JobAttemptPhase.Running,
                    RunRunning = (int)JobPhase.Running
                },
                transaction,
                cancellationToken: cancellationToken));

            results.Add(row is null
                ? new LeaseRenewalResult(
                    renewal.AttemptId,
                    false,
                    false,
                    null,
                    "attempt_or_fencing_token_mismatch")
                : new LeaseRenewalResult(
                    renewal.AttemptId,
                    true,
                    row.CancelRequested,
                    leaseExpiresAt));
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET LastHeartbeatAt = @Now
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch;",
            new { Now = now, request.WorkerId, request.SessionId, request.SessionEpoch },
            transaction,
            cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return results;
    }

    private sealed class RenewalRow
    {
        public bool CancelRequested { get; set; }
    }
}
