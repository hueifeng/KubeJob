using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<bool> PersistAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var inserted = await connection.ExecuteAsync(new CommandDefinition(@"
            INSERT INTO Kj2_CompletionIntents
                (AttemptId, RunId, WorkerId, SessionId, SessionEpoch, AttemptNumber,
                 LeaseToken, FenceVersion, Outcome, FailureCode, FailureMessage, CreatedAt)
            SELECT @AttemptId, @RunId, @WorkerId, @SessionId, @SessionEpoch, @AttemptNumber,
                   @LeaseToken, @FenceVersion, @Outcome, @FailureCode, @FailureMessage, clock_timestamp()
            WHERE EXISTS (
                SELECT 1
                FROM Kj2_JobAttempts attempt
                JOIN Kj2_JobRuns run ON run.Id = attempt.RunId
                JOIN Kj2_WorkerSessions session
                  ON session.WorkerId = @WorkerId
                 AND session.SessionId = @SessionId
                 AND session.Epoch = @SessionEpoch
                 AND session.State IN (@Ready, @Draining)
                WHERE attempt.Id = @AttemptId
                  AND attempt.RunId = @RunId
                  AND attempt.AttemptNumber = @AttemptNumber
                  AND attempt.WorkerId = @WorkerId
                  AND attempt.SessionId = @SessionId
                  AND attempt.SessionEpoch = @SessionEpoch
                  AND attempt.LeaseToken = @LeaseToken
                  AND attempt.FenceVersion = @FenceVersion
                  AND attempt.Phase = @Running
                  AND attempt.LeaseExpiresAt > clock_timestamp()
                  AND run.Phase = @RunRunning
                  AND run.CurrentAttemptId = attempt.Id
                  AND run.FenceVersion = @FenceVersion)
            ON CONFLICT (AttemptId) DO NOTHING;",
            new
            {
                request.AttemptId,
                request.RunId,
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                request.AttemptNumber,
                request.LeaseToken,
                request.FenceVersion,
                Outcome = (int)request.Outcome,
                request.FailureCode,
                request.FailureMessage,
                Ready = (int)WorkerSessionState.Ready,
                Draining = (int)WorkerSessionState.Draining,
                Running = (int)JobAttemptPhase.Running,
                RunRunning = (int)JobPhase.Running
            },
            transaction,
            cancellationToken: cancellationToken));

        if (inserted == 0)
        {
            // A repeated completion request after a lost response is accepted
            // only when it refers to the same persisted fenced intent.
            var existing = await connection.ExecuteScalarAsync<int?>(new CommandDefinition(@"
                SELECT 1
                FROM Kj2_CompletionIntents
                WHERE AttemptId = @AttemptId
                  AND FenceVersion = @FenceVersion
                  AND LeaseToken = @LeaseToken;",
                new { request.AttemptId, request.FenceVersion, request.LeaseToken },
                transaction,
                cancellationToken: cancellationToken));
            inserted = existing.HasValue ? 1 : 0;
        }

        await transaction.CommitAsync(cancellationToken);
        return inserted > 0;
    }

    public async ValueTask<IReadOnlyList<CompleteAttemptRequest>> GetPendingAsync(
        int batchSize,
        CancellationToken cancellationToken)
    {
        if (batchSize <= 0)
        {
            return Array.Empty<CompleteAttemptRequest>();
        }

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        return (await connection.QueryAsync<CompletionIntentRow>(new CommandDefinition(@"
            SELECT intent.AttemptId AS AttemptId,
                   intent.RunId AS RunId,
                   intent.WorkerId AS WorkerId,
                   intent.SessionId AS SessionId,
                   intent.SessionEpoch AS SessionEpoch,
                   intent.AttemptNumber AS AttemptNumber,
                   intent.LeaseToken AS LeaseToken,
                   intent.FenceVersion AS FenceVersion,
                   intent.Outcome AS Outcome,
                   intent.FailureCode AS FailureCode,
                   intent.FailureMessage AS FailureMessage
            FROM Kj2_CompletionIntents intent
            JOIN Kj2_JobAttempts attempt ON attempt.Id = intent.AttemptId
            JOIN Kj2_JobRuns run ON run.Id = attempt.RunId
            WHERE attempt.Phase = @Running
              AND attempt.LeaseExpiresAt > clock_timestamp()
              AND run.Phase = @RunRunning
              AND run.CurrentAttemptId = attempt.Id
              AND attempt.FenceVersion = intent.FenceVersion
              AND run.FenceVersion = intent.FenceVersion
            ORDER BY intent.CreatedAt
            LIMIT @BatchSize;",
            new
            {
                Running = (int)JobAttemptPhase.Running,
                RunRunning = (int)JobPhase.Running,
                BatchSize = batchSize
            },
            cancellationToken: cancellationToken))).Select(row => new CompleteAttemptRequest(
                row.WorkerId,
                row.SessionId,
                row.SessionEpoch,
                row.RunId,
                row.AttemptId,
                row.AttemptNumber,
                row.LeaseToken,
                row.Outcome,
                row.FailureCode,
                row.FailureMessage,
                row.FenceVersion)).ToArray();
    }

    public async ValueTask RemoveAsync(string attemptId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(attemptId);
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(
            "DELETE FROM Kj2_CompletionIntents WHERE AttemptId = @AttemptId;",
            new { AttemptId = attemptId },
            cancellationToken: cancellationToken));
    }

    private static ValueTask DeleteCompletionIntentsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyCollection<string> attemptIds,
        CancellationToken cancellationToken) =>
        attemptIds.Count == 0
            ? ValueTask.CompletedTask
            : new ValueTask(connection.ExecuteAsync(new CommandDefinition(@"
                DELETE FROM Kj2_CompletionIntents
                WHERE AttemptId = ANY(@AttemptIds);",
                new { AttemptIds = attemptIds.ToArray() },
                transaction,
                cancellationToken: cancellationToken)));

    private sealed class CompletionIntentRow
    {
        public string AttemptId { get; set; } = string.Empty;
        public string RunId { get; set; } = string.Empty;
        public string WorkerId { get; set; } = string.Empty;
        public string SessionId { get; set; } = string.Empty;
        public long SessionEpoch { get; set; }
        public int AttemptNumber { get; set; }
        public string LeaseToken { get; set; } = string.Empty;
        public long FenceVersion { get; set; }
        public JobAttemptOutcome Outcome { get; set; }
        public string? FailureCode { get; set; }
        public string? FailureMessage { get; set; }
    }
}
