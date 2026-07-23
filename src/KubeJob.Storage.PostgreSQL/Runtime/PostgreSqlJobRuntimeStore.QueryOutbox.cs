using Dapper;
using KubeJob.Core.Runtime;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<JobRunRecord?> GetRunAsync(
        string runId,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<JobRunRecord>(new CommandDefinition(@"
            SELECT *
            FROM Kj2_JobRuns
            WHERE Id = @RunId
            LIMIT 1;",
            new { RunId = runId },
            cancellationToken: cancellationToken));
    }

    public async ValueTask<IReadOnlyList<JobAttemptRecord>> GetAttemptsAsync(
        string runId,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return (await connection.QueryAsync<JobAttemptRecord>(new CommandDefinition(@"
            SELECT *
            FROM Kj2_JobAttempts
            WHERE RunId = @RunId
            ORDER BY AttemptNumber;",
            new { RunId = runId },
            cancellationToken: cancellationToken))).ToArray();
    }

    public async ValueTask<IReadOnlyList<OutboxMessageRecord>> ClaimPendingAsync(
        DateTimeOffset now,
        int batchSize,
        CancellationToken cancellationToken)
    {
        if (batchSize <= 0)
        {
            return Array.Empty<OutboxMessageRecord>();
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var messages = (await connection.QueryAsync<OutboxMessageRecord>(new CommandDefinition(@"
            SELECT *
            FROM Kj2_Outbox
            WHERE State IN (@Pending, @Failed)
              AND AvailableAt <= clock_timestamp()
            ORDER BY AvailableAt, CreatedAt
            FOR UPDATE SKIP LOCKED
            LIMIT @BatchSize;",
            new
            {
                Pending = (int)OutboxDeliveryState.Pending,
                Failed = (int)OutboxDeliveryState.Failed,
                BatchSize = batchSize
            },
            transaction,
            cancellationToken: cancellationToken))).ToArray();

        if (messages.Length > 0)
        {
            await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_Outbox
                SET State = @Publishing,
                    PublishAttempts = PublishAttempts + 1
                WHERE Id = ANY(@Ids);",
                new
                {
                    Publishing = (int)OutboxDeliveryState.Publishing,
                    Ids = messages.Select(x => x.Id).ToArray()
                },
                transaction,
                cancellationToken: cancellationToken));

            foreach (var message in messages)
            {
                message.State = OutboxDeliveryState.Publishing;
                message.PublishAttempts++;
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return messages;
    }

    public async ValueTask MarkPublishedAsync(
        string messageId,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_Outbox
            SET State = @Published,
                PublishedAt = @PublishedAt,
                LastError = NULL
            WHERE Id = @MessageId;",
            new
            {
                MessageId = messageId,
                Published = (int)OutboxDeliveryState.Published,
                PublishedAt = publishedAt
            },
            cancellationToken: cancellationToken));
    }

    public async ValueTask MarkFailedAsync(
        string messageId,
        string error,
        DateTimeOffset nextAttemptAt,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_Outbox
            SET State = @Failed,
                LastError = @Error,
                AvailableAt = @NextAttemptAt
            WHERE Id = @MessageId;",
            new
            {
                MessageId = messageId,
                Failed = (int)OutboxDeliveryState.Failed,
                Error = error,
                NextAttemptAt = nextAttemptAt
            },
            cancellationToken: cancellationToken));
    }
}
