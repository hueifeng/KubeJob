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
                  AND attempt.LeaseExpiresAt > @Now
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
                    Now = now,
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
                    "attempt_expired_or_fencing_token_mismatch")
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
