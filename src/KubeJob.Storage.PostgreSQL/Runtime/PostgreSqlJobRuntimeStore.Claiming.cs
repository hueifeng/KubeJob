using System.Text.Json;
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
        if (leaseDuration <= TimeSpan.Zero
            || maxBatchSize <= 0
            || request.AvailableSlots <= 0
            || request.Queues.Count == 0
            || request.Capabilities.Count == 0
            || request.RunIds is { Count: 0 })
        {
            return Array.Empty<ClaimedJob>();
        }

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var session = await connection.QuerySingleOrDefaultAsync<ClaimSessionRow>(new CommandDefinition(@"
            SELECT MaxConcurrency,
                   State,
                   ExecutionLane,
                   ConsumerGroup,
                   Queues::text AS QueuesJson,
                   Capabilities::text AS CapabilitiesJson
            FROM Kj2_WorkerSessions
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch
            FOR UPDATE;",
            new { request.WorkerId, request.SessionId, request.SessionEpoch },
            transaction,
            cancellationToken: cancellationToken));

        if (session is null
            || session.State != WorkerSessionState.Ready
            || !string.Equals(session.ConsumerGroup, request.ConsumerGroup, StringComparison.Ordinal)
            || !string.Equals(session.ExecutionLane, request.ExecutionLane, StringComparison.Ordinal))
        {
            await transaction.RollbackAsync(cancellationToken);
            return Array.Empty<ClaimedJob>();
        }

        var registeredQueues = (JsonSerializer.Deserialize<string[]>(session.QueuesJson, SerializerOptions)
                                ?? Array.Empty<string>())
            .ToHashSet(StringComparer.Ordinal);
        var registeredCapabilities = (JsonSerializer.Deserialize<string[]>(session.CapabilitiesJson, SerializerOptions)
                                      ?? Array.Empty<string>())
            .ToHashSet(StringComparer.Ordinal);
        var allowedQueues = request.Queues
            .Where(registeredQueues.Contains)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        var allowedCapabilities = request.Capabilities
            .Where(registeredCapabilities.Contains)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var activeAttempts = await connection.ExecuteScalarAsync<int>(new CommandDefinition(@"
            SELECT COUNT(*)
            FROM Kj2_JobAttempts
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND SessionEpoch = @SessionEpoch
              AND Phase = @Running;",
            new
            {
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                Running = (int)JobAttemptPhase.Running
            },
            transaction,
            cancellationToken: cancellationToken));

        var serverAvailable = Math.Max(0, session.MaxConcurrency - activeAttempts);
        var reportedAvailable = Math.Max(0, request.AvailableSlots);
        var limit = Math.Min(Math.Min(serverAvailable, reportedAvailable), maxBatchSize);
        if (limit == 0 || allowedQueues.Length == 0 || allowedCapabilities.Length == 0)
        {
            await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_WorkerSessions
                SET AvailableSlots = @AvailableSlots,
                    LastHeartbeatAt = clock_timestamp()
                WHERE WorkerId = @WorkerId
                  AND SessionId = @SessionId
                  AND Epoch = @SessionEpoch;",
                new
                {
                    AvailableSlots = limit == 0 ? 0 : serverAvailable,
                    request.WorkerId,
                    request.SessionId,
                    request.SessionEpoch
                },
                transaction,
                cancellationToken: cancellationToken));
            await transaction.CommitAsync(cancellationToken);
            return Array.Empty<ClaimedJob>();
        }

        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));

        var runFilter = request.RunIds is { Count: > 0 }
            ? "              AND r.Id = ANY(@RunIds)\n"
            : string.Empty;
        var candidates = (await connection.QueryAsync<JobRunRecord>(new CommandDefinition($@"
            SELECT r.*
            FROM Kj2_JobRuns r
            WHERE r.Phase = @Pending
              AND r.CancelRequested = FALSE
              AND r.ExecutionLane = @ExecutionLane
              AND r.ConsumerGroup = @ConsumerGroup
              AND r.AttemptCount < r.MaxAttempts
              AND r.AvailableAt <= @Now
              AND r.Queue = ANY(@Queues)
              AND r.JobKey = ANY(@Capabilities)
              AND (r.OrderingMode <> @KeyOrdered
                   OR r.ConcurrencyKey IS NULL
                   OR NOT EXISTS (
                       SELECT 1
                       FROM Kj2_JobRuns predecessor
                       WHERE predecessor.Queue = r.Queue
                         AND predecessor.ConcurrencyKey = r.ConcurrencyKey
                         AND predecessor.OrderingMode = @KeyOrdered
                         AND predecessor.Phase IN (@Pending, @Running)
                         AND predecessor.OrderingSequence < r.OrderingSequence))
              AND (r.OrderingMode <> @StrictFifo
                   OR NOT EXISTS (
                       SELECT 1
                       FROM Kj2_JobRuns predecessor
                       WHERE predecessor.Queue = r.Queue
                         AND predecessor.OrderingMode = @StrictFifo
                         AND predecessor.Phase IN (@Pending, @Running)
                         AND predecessor.OrderingSequence < r.OrderingSequence))
{runFilter}
            -- Soft ordering guarantee: OrderingSequence is assigned by nextval
            -- at INSERT time while READ COMMITTED visibility applies at COMMIT
            -- time. A run whose transaction commits late can therefore be
            -- admitted after a later-sequenced run was already claimed (two
            -- concurrent runs on one StrictFifo lane, or inverted KeyOrdered
            -- order) when the earlier submission's transaction spans the later
            -- submission's claim. The window is the commit gap between the two
            -- submissions; the in-memory store serializes submissions and
            -- claims under one lock and cannot exhibit it. Do not ""fix"" this
            -- by reading uncommitted rows: ordering must never depend on data
            -- the submitting transaction may still roll back.
            ORDER BY r.Priority DESC, r.AvailableAt, r.CreatedAt, r.Id
            FOR UPDATE SKIP LOCKED
            LIMIT @CandidateLimit;",
            new
            {
                Pending = (int)JobPhase.Pending,
                Running = (int)JobPhase.Running,
                KeyOrdered = (int)ExecutionOrderingMode.KeyOrdered,
                StrictFifo = (int)ExecutionOrderingMode.StrictFifo,
                Now = now,
                ExecutionLane = request.ExecutionLane,
                ConsumerGroup = request.ConsumerGroup,
                Queues = allowedQueues,
                Capabilities = allowedCapabilities,
                RunIds = request.RunIds?.ToArray(),
                CandidateLimit = Math.Min(checked(limit * 4), 4096)
            },
            transaction,
            cancellationToken: cancellationToken))).ToArray();

        var candidateConcurrencyKeys = candidates
            .Select(r => r.ConcurrencyKey)
            .Where(k => !string.IsNullOrWhiteSpace(k))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        var runningConcurrencyKeys = candidateConcurrencyKeys.Length == 0
            ? new HashSet<string>(StringComparer.Ordinal)
            : (await connection.QueryAsync<string>(new CommandDefinition(@"
                SELECT DISTINCT ConcurrencyKey
                FROM Kj2_JobRuns
                WHERE Phase = @Running
                  AND ConcurrencyKey = ANY(@Keys);",
                new { Running = (int)JobPhase.Running, Keys = candidateConcurrencyKeys },
                transaction,
                cancellationToken: cancellationToken))).ToHashSet(StringComparer.Ordinal);

        var reserved = new List<JobRunRecord>(Math.Min(limit, candidates.Length));
        var reservedConcurrencyKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var run in candidates)
        {
            if (reserved.Count >= limit)
            {
                break;
            }

            if (!string.IsNullOrWhiteSpace(run.ConcurrencyKey)
                && reservedConcurrencyKeys.Contains(run.ConcurrencyKey))
            {
                continue;
            }

            if (!await TryReserveConcurrencyKeyAsync(connection, transaction, run, runningConcurrencyKeys, cancellationToken))
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(run.ConcurrencyKey))
            {
                reservedConcurrencyKeys.Add(run.ConcurrencyKey);
            }

            reserved.Add(run);
        }

        List<ClaimedJob> claimed;
        if (reserved.Count == 0)
        {
            claimed = new List<ClaimedJob>();
        }
        else if (reserved.Count == 1)
        {
            claimed = new List<ClaimedJob>(1);
            var job = await ClaimSingleAsync(connection, transaction, reserved[0], request, now, leaseDuration, cancellationToken);
            if (job is not null)
            {
                claimed.Add(job);
            }
        }
        else
        {
            claimed = await ClaimBatchAsync(connection, transaction, reserved, request, now, leaseDuration, cancellationToken);
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET AvailableSlots = GREATEST(0, @ServerAvailable - @ClaimedCount),
                LastHeartbeatAt = @Now
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch;",
            new
            {
                ServerAvailable = serverAvailable,
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

    private static async ValueTask<ClaimedJob?> ClaimSingleAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        JobRunRecord run,
        ClaimJobsRequest request,
        DateTimeOffset now,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
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
            return null;
        }

        return new ClaimedJob(
            run.Id,
            attemptId,
            attemptNumber,
            leaseToken,
            leaseExpiresAt,
            run.JobKey,
            run.PayloadJson,
            run.Queue,
            run.TimeoutSeconds,
            run.OrderingMode,
            run.AvailableAt);
    }

    private static async ValueTask<List<ClaimedJob>> ClaimBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<JobRunRecord> reserved,
        ClaimJobsRequest request,
        DateTimeOffset now,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        var leaseExpiresAt = now.Add(leaseDuration);
        var items = reserved
            .Select(run => (
                Run: run,
                AttemptId: NewId(),
                AttemptNumber: run.AttemptCount + 1,
                LeaseToken: NewId()))
            .ToArray();

        await connection.ExecuteAsync(new CommandDefinition(@"
            INSERT INTO Kj2_JobAttempts
                (Id, RunId, AttemptNumber, WorkerId, SessionId, SessionEpoch,
                 LeaseToken, Phase, ClaimedAt, StartedAt, LeaseExpiresAt)
            SELECT item.Id, item.RunId, item.AttemptNumber, @WorkerId, @SessionId, @SessionEpoch,
                   item.LeaseToken, @Phase, @ClaimedAt, @ClaimedAt, item.LeaseExpiresAt
            FROM unnest(
                CAST(@AttemptIds AS text[]),
                CAST(@RunIds AS text[]),
                CAST(@AttemptNumbers AS int[]),
                CAST(@LeaseTokens AS text[]),
                CAST(@LeaseExpiresAts AS timestamptz[]))
                AS item(Id, RunId, AttemptNumber, LeaseToken, LeaseExpiresAt);",
            new
            {
                AttemptIds = items.Select(x => x.AttemptId).ToArray(),
                RunIds = items.Select(x => x.Run.Id).ToArray(),
                AttemptNumbers = items.Select(x => x.AttemptNumber).ToArray(),
                LeaseTokens = items.Select(x => x.LeaseToken).ToArray(),
                LeaseExpiresAts = items.Select(_ => leaseExpiresAt).ToArray(),
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                Phase = (int)JobAttemptPhase.Running,
                ClaimedAt = now
            },
            transaction,
            cancellationToken: cancellationToken));

        var updatedRunIds = (await connection.QueryAsync<string>(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET Phase = @Running,
                AttemptCount = item.AttemptNumber,
                CurrentAttemptId = item.AttemptId,
                CurrentWorkerId = @WorkerId,
                CurrentSessionId = @SessionId,
                StartedAt = COALESCE(StartedAt, @StartedAt),
                Version = Version + 1
            FROM unnest(
                CAST(@RunIds AS text[]),
                CAST(@AttemptIds AS text[]),
                CAST(@AttemptNumbers AS int[]))
                AS item(RunId, AttemptId, AttemptNumber)
            WHERE Kj2_JobRuns.Id = item.RunId
              AND Kj2_JobRuns.Phase = @Pending
              AND Kj2_JobRuns.CancelRequested = FALSE
            RETURNING Kj2_JobRuns.Id;",
            new
            {
                RunIds = items.Select(x => x.Run.Id).ToArray(),
                AttemptIds = items.Select(x => x.AttemptId).ToArray(),
                AttemptNumbers = items.Select(x => x.AttemptNumber).ToArray(),
                Running = (int)JobPhase.Running,
                Pending = (int)JobPhase.Pending,
                request.WorkerId,
                request.SessionId,
                StartedAt = now
            },
            transaction,
            cancellationToken: cancellationToken))).ToHashSet(StringComparer.Ordinal);

        var losers = items.Where(x => !updatedRunIds.Contains(x.Run.Id)).ToArray();
        if (losers.Length > 0)
        {
            await connection.ExecuteAsync(new CommandDefinition(
                "DELETE FROM Kj2_JobAttempts WHERE Id = ANY(@AttemptIds);",
                new { AttemptIds = losers.Select(x => x.AttemptId).ToArray() },
                transaction,
                cancellationToken: cancellationToken));
        }

        var claimed = new List<ClaimedJob>(items.Length);
        foreach (var item in items)
        {
            if (!updatedRunIds.Contains(item.Run.Id))
            {
                continue;
            }

            claimed.Add(new ClaimedJob(
                item.Run.Id,
                item.AttemptId,
                item.AttemptNumber,
                item.LeaseToken,
                leaseExpiresAt,
                item.Run.JobKey,
                item.Run.PayloadJson,
                item.Run.Queue,
                item.Run.TimeoutSeconds,
                item.Run.OrderingMode,
                item.Run.AvailableAt));
        }

        return claimed;
    }

    private static async ValueTask<bool> TryReserveConcurrencyKeyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        JobRunRecord run,
        HashSet<string> runningConcurrencyKeys,
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

        if (runningConcurrencyKeys.Contains(run.ConcurrencyKey))
        {
            return false;
        }

        // The runningConcurrencyKeys snapshot was taken before this lock was
        // acquired, so a concurrent claim of the same key may have committed
        // in between and released the advisory lock. Holding the lock now
        // guarantees no other claimant is in flight, so a committed Running
        // row is the only remaining overlap to detect; re-verify instead of
        // trusting the stale snapshot.
        var runningNow = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(@"
            SELECT EXISTS (
                SELECT 1
                FROM Kj2_JobRuns
                WHERE ConcurrencyKey = @ConcurrencyKey
                  AND Phase = @Running);",
            new
            {
                run.ConcurrencyKey,
                Running = (int)JobPhase.Running
            },
            transaction,
            cancellationToken: cancellationToken));
        return !runningNow;
    }

    private sealed class ClaimSessionRow
    {
        public int MaxConcurrency { get; set; }
        public WorkerSessionState State { get; set; }
        public string ExecutionLane { get; set; } = "default";
        public string ConsumerGroup { get; set; } = "default";
        public string QueuesJson { get; set; } = "[]";
        public string CapabilitiesJson { get; set; } = "[]";
    }
}
