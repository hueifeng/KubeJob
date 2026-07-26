using Dapper;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Npgsql;

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
        var effectiveNow = now.ToUniversalTime() > databaseNow
            ? now.ToUniversalTime()
            : databaseNow;
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
                Now = effectiveNow,
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

    public async ValueTask MarkAbandonedAsync(
        OutboxFailure failure,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_Outbox
            SET State = @Abandoned,
                LastError = @Error,
                ClaimToken = NULL
            WHERE Id = @MessageId
              AND State = @Publishing
              AND ClaimToken = @ClaimToken;",
            new
            {
                MessageId = failure.MessageId,
                Abandoned = (int)OutboxDeliveryState.Abandoned,
                Publishing = (int)OutboxDeliveryState.Publishing,
                ClaimToken = failure.ClaimToken,
                Error = failure.Error
            },
            cancellationToken: cancellationToken));
    }

    public async ValueTask<OutboxDispatchBatch> DispatchOnceAsync(
        TimeSpan claimDuration,
        TimeSpan retryDelay,
        int batchSize,
        Func<OutboxMessageRecord, CancellationToken, ValueTask> dispatch,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dispatch);

        if (claimDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(claimDuration));
        }

        if (retryDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retryDelay));
        }

        if (batchSize <= 0)
        {
            return new OutboxDispatchBatch(Array.Empty<string>(), Array.Empty<string>());
        }

        var dispatched = new List<string>(batchSize);
        var failed = new List<string>(batchSize);
        var abandoned = new List<string>(batchSize);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);

        for (var iteration = 0; iteration < batchSize; iteration++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            OutboxMessageRecord? message;
            string claimToken;

            // Claim and commit before touching the broker. The database lock is
            // therefore held only for the short state transition, not for the
            // network round-trip or publisher confirm.
            await using (var claimTransaction = await connection.BeginTransactionAsync(cancellationToken))
            {
                message = await connection.QuerySingleOrDefaultAsync<OutboxMessageRecord>(new CommandDefinition(@"
                    SELECT *
                    FROM Kj2_Outbox
                    WHERE State IN (@Pending, @Failed, @Publishing)
                      AND AvailableAt <= clock_timestamp()
                    ORDER BY AvailableAt, CreatedAt
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1;",
                    new
                    {
                        Pending = (int)OutboxDeliveryState.Pending,
                        Failed = (int)OutboxDeliveryState.Failed,
                        Publishing = (int)OutboxDeliveryState.Publishing
                    },
                    claimTransaction,
                    cancellationToken: cancellationToken));

                if (message is null)
                {
                    await claimTransaction.CommitAsync(cancellationToken);
                    break;
                }

                var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
                    "SELECT clock_timestamp();",
                    transaction: claimTransaction,
                    cancellationToken: cancellationToken));
                claimToken = NewId();
                var claimUntil = now.Add(claimDuration);

                await connection.ExecuteAsync(new CommandDefinition(@"
                    UPDATE Kj2_Outbox
                    SET State = @Publishing,
                        PublishAttempts = PublishAttempts + 1,
                        AvailableAt = @ClaimUntil,
                        ClaimToken = @ClaimToken
                    WHERE Id = @MessageId;",
                    new
                    {
                        MessageId = message.Id,
                        Publishing = (int)OutboxDeliveryState.Publishing,
                        ClaimUntil = claimUntil,
                        ClaimToken = claimToken
                    },
                    claimTransaction,
                    cancellationToken: cancellationToken));
                await claimTransaction.CommitAsync(cancellationToken);

                message.State = OutboxDeliveryState.Publishing;
                message.PublishAttempts++;
                message.AvailableAt = claimUntil;
                message.ClaimToken = claimToken;
            }

            try
            {
                await dispatch(message, cancellationToken).ConfigureAwait(false);
                await using var publishTransaction = await connection.BeginTransactionAsync(cancellationToken);
                await connection.ExecuteAsync(new CommandDefinition(@"
                    UPDATE Kj2_Outbox
                    SET State = @Published,
                        PublishedAt = clock_timestamp(),
                        LastError = NULL,
                        ClaimToken = NULL
                    WHERE Id = @MessageId
                      AND State = @Publishing
                      AND ClaimToken = @ClaimToken;",
                    new
                    {
                        MessageId = message.Id,
                        Published = (int)OutboxDeliveryState.Published,
                        Publishing = (int)OutboxDeliveryState.Publishing,
                        ClaimToken = claimToken
                    },
                    publishTransaction,
                    cancellationToken: cancellationToken));
                await publishTransaction.CommitAsync(cancellationToken);
                dispatched.Add(message.Id);
            }
            catch (PermanentOutboxException ex)
            {
                await MarkAbandonedAsync(
                    new OutboxFailure(
                        message.Id,
                        claimToken,
                        ex.Message,
                        DateTimeOffset.UtcNow),
                    CancellationToken.None);
                abandoned.Add(message.Id);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                await MarkDispatchFailedAsync(
                    connection,
                    message.Id,
                    claimToken,
                    "publisher_canceled",
                    retryDelay);
                failed.Add(message.Id);
                throw;
            }
            catch (Exception ex)
            {
                await MarkDispatchFailedAsync(
                    connection,
                    message.Id,
                    claimToken,
                    ex.Message,
                    retryDelay);
                failed.Add(message.Id);
            }
        }

        return new OutboxDispatchBatch(dispatched, failed, abandoned);
    }

    private static async ValueTask MarkDispatchFailedAsync(
        NpgsqlConnection connection,
        string messageId,
        string claimToken,
        string error,
        TimeSpan retryDelay)
    {
        await using var transaction = await connection.BeginTransactionAsync(CancellationToken.None);
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_Outbox
            SET State = @Failed,
                LastError = @Error,
                AvailableAt = clock_timestamp() + @RetryInterval,
                ClaimToken = NULL
            WHERE Id = @MessageId
              AND State = @Publishing
              AND ClaimToken = @ClaimToken;",
            new
            {
                MessageId = messageId,
                Failed = (int)OutboxDeliveryState.Failed,
                Publishing = (int)OutboxDeliveryState.Publishing,
                ClaimToken = claimToken,
                Error = error,
                RetryInterval = retryDelay
            },
            transaction,
            cancellationToken: CancellationToken.None));
        await transaction.CommitAsync(CancellationToken.None);
    }
}
