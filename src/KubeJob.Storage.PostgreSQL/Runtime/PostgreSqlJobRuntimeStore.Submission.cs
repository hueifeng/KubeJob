using System.Text.Json;
using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<SubmitJobResult> SubmitAsync(
        SubmitJobCommand command,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        if (!string.IsNullOrWhiteSpace(command.IdempotencyKey))
        {
            var existing = await connection.QuerySingleOrDefaultAsync<JobRunRecord>(new CommandDefinition(@"
                SELECT *
                FROM Kj2_JobRuns
                WHERE IdempotencyKey = @IdempotencyKey
                LIMIT 1;",
                new { command.IdempotencyKey },
                transaction,
                cancellationToken: cancellationToken));

            if (existing is not null)
            {
                JobSubmissionIdentity.EnsureCompatible(existing, command);
                await transaction.CommitAsync(cancellationToken);
                return new SubmitJobResult(existing, Existing: true);
            }
        }

        var now = await GetDatabaseNowAsync(connection, transaction, cancellationToken);
        var run = new JobRunRecord
        {
            Id = NewId(),
            JobKey = command.JobKey,
            PayloadJson = command.PayloadJson,
            Queue = command.Queue,
            Priority = command.Priority,
            Phase = JobPhase.Pending,
            AvailableAt = command.AvailableAt.ToUniversalTime(),
            CreatedAt = now,
            MaxAttempts = command.MaxAttempts,
            TimeoutSeconds = command.TimeoutSeconds,
            IdempotencyKey = command.IdempotencyKey,
            ConcurrencyKey = command.ConcurrencyKey
        };

        var inserted = await connection.ExecuteAsync(new CommandDefinition(@"
            INSERT INTO Kj2_JobRuns
                (Id, JobKey, PayloadJson, Queue, Priority, Phase, AvailableAt,
                 CreatedAt, AttemptCount, MaxAttempts, TimeoutSeconds,
                 IdempotencyKey, ConcurrencyKey, CancelRequested, Version)
            VALUES
                (@Id, @JobKey, CAST(@PayloadJson AS jsonb), @Queue, @Priority,
                 @Phase, @AvailableAt, @CreatedAt, 0, @MaxAttempts,
                 @TimeoutSeconds, @IdempotencyKey, @ConcurrencyKey, FALSE, 0)
            ON CONFLICT (IdempotencyKey) DO NOTHING;",
            new
            {
                run.Id,
                run.JobKey,
                run.PayloadJson,
                run.Queue,
                run.Priority,
                Phase = (int)run.Phase,
                run.AvailableAt,
                run.CreatedAt,
                run.MaxAttempts,
                run.TimeoutSeconds,
                run.IdempotencyKey,
                run.ConcurrencyKey
            },
            transaction,
            cancellationToken: cancellationToken));

        if (inserted == 0)
        {
            var existing = await connection.QuerySingleAsync<JobRunRecord>(new CommandDefinition(@"
                SELECT *
                FROM Kj2_JobRuns
                WHERE IdempotencyKey = @IdempotencyKey
                LIMIT 1;",
                new { command.IdempotencyKey },
                transaction,
                cancellationToken: cancellationToken));
            JobSubmissionIdentity.EnsureCompatible(existing, command);
            await transaction.CommitAsync(cancellationToken);
            return new SubmitJobResult(existing, Existing: true);
        }

        await AddOutboxAsync(
            connection,
            transaction,
            run.Queue,
            OutboxEventTypes.WorkAvailable,
            JsonSerializer.Serialize(new { runId = run.Id, queue = run.Queue }, SerializerOptions),
            run.AvailableAt,
            cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new SubmitJobResult(run, Existing: false);
    }

    public async ValueTask<bool> RequeueWorkAvailableAsync(
        string runId,
        DateTimeOffset availableAt,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var state = await connection.QuerySingleOrDefaultAsync<WorkRequeueState>(new CommandDefinition(@"
            SELECT Phase, CancelRequested, AvailableAt
            FROM Kj2_JobRuns
            WHERE Id = @RunId
            FOR UPDATE;",
            new { RunId = runId },
            transaction,
            cancellationToken: cancellationToken));

        if (state is null
            || state.CancelRequested
            || state.Phase != (int)JobPhase.Pending)
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        var retryAt = state.AvailableAt > availableAt
            ? state.AvailableAt
            : availableAt;
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET AvailableAt = @AvailableAt,
                Version = Version + 1
            WHERE Id = @RunId
              AND Phase = @Pending
              AND CancelRequested = FALSE;",
            new
            {
                RunId = runId,
                AvailableAt = retryAt,
                Pending = (int)JobPhase.Pending
            },
            transaction,
            cancellationToken: cancellationToken));

        var queue = await connection.QuerySingleAsync<string>(new CommandDefinition(@"
            SELECT Queue
            FROM Kj2_JobRuns
            WHERE Id = @RunId;",
            new { RunId = runId },
            transaction,
            cancellationToken: cancellationToken));
        await AddOutboxAsync(
            connection,
            transaction,
            queue,
            OutboxEventTypes.WorkAvailable,
            JsonSerializer.Serialize(new { runId, queue }, SerializerOptions),
            retryAt,
            cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async ValueTask<CancelJobResult> RequestCancelAsync(
        string runId,
        string? reason,
        string? consumerGroup,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var state = await connection.QuerySingleOrDefaultAsync<RunCancelState>(new CommandDefinition(@"
            SELECT Id AS RunId,
                   Queue,
                   Phase,
                   CancelRequested
            FROM Kj2_JobRuns
            WHERE Id = @RunId
            FOR UPDATE;",
            new { RunId = runId },
            transaction,
            cancellationToken: cancellationToken));

        if (state is null)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new CancelJobResult(false, null, null);
        }

        if (state.CancelRequested
            || state.Phase is (int)JobPhase.Succeeded
                or (int)JobPhase.Failed
                or (int)JobPhase.Canceled
                or (int)JobPhase.Dead)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new CancelJobResult(false, state.Queue, consumerGroup);
        }

        var databaseNow = await GetDatabaseNowAsync(connection, transaction, cancellationToken);

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET CancelRequested = TRUE,
                FailureCode = 'cancel_requested',
                FailureMessage = @Reason,
                Phase = CASE WHEN Phase = @Pending THEN @Canceled ELSE Phase END,
                CompletedAt = CASE WHEN Phase = @Pending THEN @Now ELSE CompletedAt END,
                Version = Version + 1
            WHERE Id = @RunId
              AND Phase IN (@Pending, @Running)
              AND CancelRequested = FALSE;",
            new
            {
                RunId = runId,
                Reason = reason,
                Pending = (int)JobPhase.Pending,
                Running = (int)JobPhase.Running,
                Canceled = (int)JobPhase.Canceled,
                Now = databaseNow
            },
            transaction,
            cancellationToken: cancellationToken));

        if (!string.IsNullOrWhiteSpace(consumerGroup)
            && !string.IsNullOrWhiteSpace(state.Queue))
        {
            await AddCancelOutboxAsync(
                connection,
                transaction,
                consumerGroup!,
                runId,
                databaseNow,
                cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return new CancelJobResult(true, state.Queue, consumerGroup);
    }

    private sealed class WorkRequeueState
    {
        public int Phase { get; set; }
        public bool CancelRequested { get; set; }
        public DateTimeOffset AvailableAt { get; set; }
    }

    private sealed class RunCancelState
    {
        public string RunId { get; set; } = string.Empty;
        public string Queue { get; set; } = string.Empty;
        public int Phase { get; set; }
        public bool CancelRequested { get; set; }
    }
}
