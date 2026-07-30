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

        if (session is null || session.State != WorkerSessionState.Ready)
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
              AND r.AttemptCount < r.MaxAttempts
              AND r.AvailableAt <= @Now
              AND r.Queue = ANY(@Queues)
              AND r.JobKey = ANY(@Capabilities)
{runFilter}
            ORDER BY r.Priority DESC, r.AvailableAt, r.CreatedAt, r.Id
            FOR UPDATE SKIP LOCKED
            LIMIT @CandidateLimit;",
            new
            {
                Pending = (int)JobPhase.Pending,
                Now = now,
                Queues = allowedQueues,
                Capabilities = allowedCapabilities,
                RunIds = request.RunIds?.ToArray(),
                CandidateLimit = Math.Min(checked(limit * 4), 4096)
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

    private sealed class ClaimSessionRow
    {
        public int MaxConcurrency { get; set; }
        public WorkerSessionState State { get; set; }
        public string QueuesJson { get; set; } = "[]";
        public string CapabilitiesJson { get; set; } = "[]";
    }
}
