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
                await transaction.CommitAsync(cancellationToken);
                return new SubmitJobResult(existing, Existing: true);
            }
        }

        var now = DateTimeOffset.UtcNow;
        var run = new JobRunRecord
        {
            Id = NewId(),
            JobKey = command.JobKey,
            PayloadJson = command.PayloadJson,
            Queue = command.Queue,
            Priority = command.Priority,
            Phase = JobPhase.Pending,
            AvailableAt = command.AvailableAt,
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
            await transaction.CommitAsync(cancellationToken);
            return new SubmitJobResult(existing, Existing: true);
        }

        await AddOutboxAsync(
            connection,
            transaction,
            run.Id,
            run.Queue,
            run.AvailableAt,
            cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new SubmitJobResult(run, Existing: false);
    }

    public async ValueTask<bool> RequestCancelAsync(
        string runId,
        string? reason,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var now = DateTimeOffset.UtcNow;
        var affected = await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET CancelRequested = TRUE,
                FailureCode = 'cancel_requested',
                FailureMessage = @Reason,
                Phase = CASE WHEN Phase = @Pending THEN @Canceled ELSE Phase END,
                CompletedAt = CASE WHEN Phase = @Pending THEN @Now ELSE CompletedAt END,
                Version = Version + 1
            WHERE Id = @RunId
              AND Phase IN (@Pending, @Running);",
            new
            {
                RunId = runId,
                Reason = reason,
                Now = now,
                Pending = (int)JobPhase.Pending,
                Running = (int)JobPhase.Running,
                Canceled = (int)JobPhase.Canceled
            },
            cancellationToken: cancellationToken));
        return affected > 0;
    }
}
