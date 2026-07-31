using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<IReadOnlyList<LeaseRenewalResult>> RenewLeasesAsync(
        RenewLeasesRequest request,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        if (request.Attempts.Count == 0)
        {
            return Array.Empty<LeaseRenewalResult>();
        }

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var sessionValid = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(@"
            SELECT EXISTS (
                SELECT 1
                FROM Kj2_WorkerSessions
                WHERE WorkerId = @WorkerId
                  AND SessionId = @SessionId
                  AND Epoch = @SessionEpoch
                  AND State NOT IN (@Closed, @Stale)
                FOR UPDATE
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
                .Select(x => new LeaseRenewalResult(
                    x.AttemptId,
                    false,
                    false,
                    null,
                    "stale_worker_session"))
                .ToArray();
        }

        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));
        var leaseExpiresAt = now.Add(leaseDuration);

        var renewed = (await connection.QueryAsync<RenewalRow>(new CommandDefinition(@"
            UPDATE Kj2_JobAttempts attempt
            SET LeaseExpiresAt = @LeaseExpiresAt
            FROM Kj2_JobRuns run,
                 unnest(CAST(@AttemptIds AS text[]), CAST(@LeaseTokens AS text[]))
                     AS renewal(AttemptId, LeaseToken)
            WHERE attempt.Id = renewal.AttemptId
              AND attempt.LeaseToken = renewal.LeaseToken
              AND attempt.RunId = run.Id
              AND attempt.Phase = @AttemptRunning
              AND attempt.LeaseExpiresAt > @Now
              AND attempt.WorkerId = @WorkerId
              AND attempt.SessionId = @SessionId
              AND attempt.SessionEpoch = @SessionEpoch
              AND EXISTS (
                  SELECT 1
                  FROM Kj2_WorkerSessions worker_session
                  WHERE worker_session.WorkerId = @WorkerId
                    AND worker_session.SessionId = @SessionId
                    AND worker_session.Epoch = @SessionEpoch
                    AND worker_session.State IN (@Ready, @Draining)
              )
              AND run.Phase = @RunRunning
              AND run.CurrentAttemptId = attempt.Id
            RETURNING attempt.Id AS AttemptId, run.CancelRequested;",
            new
            {
                AttemptIds = request.Attempts.Select(x => x.AttemptId).ToArray(),
                LeaseTokens = request.Attempts.Select(x => x.LeaseToken).ToArray(),
                Now = now,
                LeaseExpiresAt = leaseExpiresAt,
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                Ready = (int)WorkerSessionState.Ready,
                Draining = (int)WorkerSessionState.Draining,
                AttemptRunning = (int)JobAttemptPhase.Running,
                RunRunning = (int)JobPhase.Running
            },
            transaction,
            cancellationToken: cancellationToken))).ToDictionary(row => row.AttemptId, StringComparer.Ordinal);

        var results = new List<LeaseRenewalResult>(request.Attempts.Count);
        foreach (var renewal in request.Attempts)
        {
            results.Add(renewed.TryGetValue(renewal.AttemptId, out var row)
                ? new LeaseRenewalResult(
                    renewal.AttemptId,
                    true,
                    row.CancelRequested,
                    leaseExpiresAt)
                : new LeaseRenewalResult(
                    renewal.AttemptId,
                    false,
                    false,
                    null,
                    "attempt_expired_or_fencing_token_mismatch"));
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET LastHeartbeatAt = @Now
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch
              AND State IN (@Ready, @Draining);",
            new
            {
                Now = now,
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                Ready = (int)WorkerSessionState.Ready,
                Draining = (int)WorkerSessionState.Draining
            },
            transaction,
            cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return results;
    }

    private sealed class RenewalRow
    {
        public string AttemptId { get; set; } = string.Empty;

        public bool CancelRequested { get; set; }
    }
}
