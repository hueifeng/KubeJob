using Dapper;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

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
        TimeSpan claimDuration,
        int batchSize,
        CancellationToken cancellationToken)
    {
        _ = now;
        if (claimDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(claimDuration));
        }

        if (batchSize <= 0)
        {
            return Array.Empty<OutboxMessageRecord>();
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var databaseNow = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));
        var claimUntil = databaseNow.Add(claimDuration);

        var messages = (await connection.QueryAsync<OutboxMessageRecord>(new CommandDefinition(@"
            SELECT *
            FROM Kj2_Outbox
            WHERE State IN (@Pending, @Failed, @Publishing)
              AND AvailableAt <= @Now
            ORDER BY AvailableAt, CreatedAt
            FOR UPDATE SKIP LOCKED
            LIMIT @BatchSize;",
            new
            {
                Pending = (int)OutboxDeliveryState.Pending,
                Failed = (int)OutboxDeliveryState.Failed,
                Publishing = (int)OutboxDeliveryState.Publishing,
                Now = databaseNow,
                BatchSize = batchSize
            },
            transaction,
            cancellationToken: cancellationToken))).ToArray();

        if (messages.Length > 0)
        {
            var claimTokens = messages
                .Select(_ => NewId())
                .ToArray();

            await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_Outbox outbox
                SET State = @Publishing,
                    AvailableAt = @ClaimUntil,
                    PublishAttempts = outbox.PublishAttempts + 1,
                    ClaimToken = claimed.ClaimToken
                FROM unnest(
                    CAST(@Ids AS text[]),
                    CAST(@ClaimTokens AS text[]))
                    AS claimed(Id, ClaimToken)
                WHERE outbox.Id = claimed.Id;",
                new
                {
                    Publishing = (int)OutboxDeliveryState.Publishing,
                    ClaimUntil = claimUntil,
                    Ids = messages.Select(x => x.Id).ToArray(),
                    ClaimTokens = claimTokens
                },
                transaction,
                cancellationToken: cancellationToken));

            for (var index = 0; index < messages.Length; index++)
            {
                var message = messages[index];
                message.State = OutboxDeliveryState.Publishing;
                message.AvailableAt = claimUntil;
                message.PublishAttempts++;
                message.ClaimToken = claimTokens[index];
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return messages;
    }

    public async ValueTask MarkPublishedAsync(
        IReadOnlyList<OutboxPublication> publications,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken)
    {
        if (publications.Count == 0)
        {
            return;
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_Outbox outbox
            SET State = @Published,
                PublishedAt = @PublishedAt,
                LastError = NULL,
                ClaimToken = NULL
            FROM unnest(
                CAST(@Ids AS text[]),
                CAST(@ClaimTokens AS text[]))
                AS completed(Id, ClaimToken)
            WHERE outbox.Id = completed.Id
              AND outbox.State = @Publishing
              AND outbox.ClaimToken = completed.ClaimToken;",
            new
            {
                Published = (int)OutboxDeliveryState.Published,
                Publishing = (int)OutboxDeliveryState.Publishing,
                PublishedAt = publishedAt.ToUniversalTime(),
                Ids = publications.Select(x => x.MessageId).ToArray(),
                ClaimTokens = publications.Select(x => x.ClaimToken).ToArray()
            },
            cancellationToken: cancellationToken));
    }

    public async ValueTask MarkFailedAsync(
        OutboxFailure failure,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_Outbox
            SET State = @Failed,
                LastError = @Error,
                AvailableAt = @NextAttemptAt,
                ClaimToken = NULL
            WHERE Id = @MessageId
              AND State = @Publishing
              AND ClaimToken = @ClaimToken;",
            new
            {
                MessageId = failure.MessageId,
                Failed = (int)OutboxDeliveryState.Failed,
                Publishing = (int)OutboxDeliveryState.Publishing,
                ClaimToken = failure.ClaimToken,
                Error = failure.Error,
                NextAttemptAt = failure.NextAttemptAt.ToUniversalTime()
            },
            cancellationToken: cancellationToken));
    }
}
