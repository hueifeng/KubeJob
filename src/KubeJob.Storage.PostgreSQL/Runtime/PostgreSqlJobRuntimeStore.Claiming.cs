using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
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

        var candidates = (await connection.QueryAsync<JobRunRecord>(new CommandDefinition(@"
            SELECT r.*
            FROM Kj2_JobRuns r
            WHERE r.Phase = @Pending
              AND r.CancelRequested = FALSE
              AND r.AttemptCount < r.MaxAttempts
              AND r.AvailableAt <= @Now
              AND r.Queue = ANY(@Queues)
              AND r.JobKey = ANY(@Capabilities)
            ORDER BY r.Priority DESC, r.AvailableAt, r.CreatedAt, r.Id
            FOR UPDATE SKIP LOCKED
            LIMIT @CandidateLimit;",
            new
            {
                Pending = (int)JobPhase.Pending,
                Now = now,
                Queues = request.Queues.ToArray(),
                Capabilities = request.Capabilities.ToArray(),
                CandidateLimit = Math.Min(limit * 4, 4096)
            },
            transaction,
            cancellationToken: cancellationToken))).ToArray();

        var claimed = new List<ClaimedJob>(Math.Min(limit, candidates.Length));
        foreach (var run in candidates)
        {
            if (claimed.Count >= limit)
            {
                break;
            }

            if (!await TryReserveConcurrencyKeyAsync(connection, transaction, run, cancellationToken))
            {
                continue;
            }

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

            var updated = await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_JobRuns
                SET Phase = @Running,
                    AttemptCount = @AttemptNumber,
                    CurrentAttemptId = @AttemptId,
                    CurrentWorkerId = @WorkerId,
                    CurrentSessionId = @SessionId,
                    StartedAt = COALESCE(StartedAt, @StartedAt),
                    Version = Version + 1
                WHERE Id = @RunId
                  AND Phase = @Pending
                  AND CancelRequested = FALSE;",
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

            if (updated == 0)
            {
                await connection.ExecuteAsync(new CommandDefinition(
                    "DELETE FROM Kj2_JobAttempts WHERE Id = @AttemptId;",
                    new { AttemptId = attemptId },
                    transaction,
                    cancellationToken: cancellationToken));
                continue;
            }

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

    private static async ValueTask<bool> TryReserveConcurrencyKeyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        JobRunRecord run,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(run.ConcurrencyKey))
        {
            return true;
        }

        var locked = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(
            "SELECT pg_try_advisory_xact_lock(hashtext(@ConcurrencyKey));",
            new { run.ConcurrencyKey },
            transaction,
            cancellationToken: cancellationToken));
        if (!locked)
        {
            return false;
        }

        return !await connection.ExecuteScalarAsync<bool>(new CommandDefinition(@"
            SELECT EXISTS (
                SELECT 1
                FROM Kj2_JobRuns
                WHERE Id <> @RunId
                  AND Phase = @Running
                  AND ConcurrencyKey = @ConcurrencyKey
            );",
            new
            {
                RunId = run.Id,
                Running = (int)JobPhase.Running,
                run.ConcurrencyKey
            },
            transaction,
            cancellationToken: cancellationToken));
    }
}
