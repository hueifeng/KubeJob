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
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
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
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
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

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
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

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
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
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
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
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
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

        // Reuse a single connection for the whole batch so we don't pay the
        // connect / TLS handshake on every message. If a connection-level
        // exception kills the loop, any row we already dispatched is durably
        // Published; the broker reconciles the rest on the next poll.
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        NpgsqlConnection? connection = null;
        try
        {
            connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);

            for (var iteration = 0; iteration < batchSize; iteration++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var claimed = await ClaimNextMessageAsync(connection, claimDuration, cancellationToken);
                if (claimed is null)
                {
                    break;
                }

                try
                {
                    await dispatch(claimed, cancellationToken).ConfigureAwait(false);
                    await MarkPublishedAsync(connection, claimed.Id, claimed.ClaimToken!, cancellationToken);
                    dispatched.Add(claimed.Id);
                }
                catch (PermanentOutboxException ex)
                {
                    // Abandon rows even when the host is shutting down; the
                    // 5-second timeout caps the work so we don't block the
                    // shutdown signal indefinitely.
                    await TryAbandonAsync(
                        new OutboxFailure(
                            claimed.Id,
                            claimed.ClaimToken!,
                            ex.Message,
                            DateTimeOffset.UtcNow.Add(retryDelay)));
                    abandoned.Add(claimed.Id);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    await TryMarkDispatchFailedAsync(
                        connection,
                        claimed.Id,
                        claimed.ClaimToken!,
                        "publisher_canceled",
                        retryDelay);
                    failed.Add(claimed.Id);
                    throw;
                }
                catch (Exception ex)
                {
                    await TryMarkDispatchFailedAsync(
                        connection,
                        claimed.Id,
                        claimed.ClaimToken!,
                        ex.Message,
                        retryDelay);
                    failed.Add(claimed.Id);
                }
            }
        }
        finally
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }
        }

        return new OutboxDispatchBatch(dispatched, failed, abandoned);
    }

    private async ValueTask<OutboxMessageRecord?> ClaimNextMessageAsync(
        NpgsqlConnection connection,
        TimeSpan claimDuration,
        CancellationToken cancellationToken)
    {
        // Atomically pick the next claimable row and flip it to Publishing.
        // RETURNING hands back the row + its fresh ClaimToken so we can hand
        // it to the dispatcher without a second SELECT.
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            var claimToken = NewId();
            var claimed = await connection.QuerySingleOrDefaultAsync<OutboxClaimRow>(new CommandDefinition(@"
                UPDATE Kj2_Outbox
                SET State = @Publishing,
                    PublishAttempts = PublishAttempts + 1,
                    AvailableAt = clock_timestamp() + @ClaimDuration,
                    ClaimToken = @ClaimToken
                WHERE Id = (
                    SELECT Id
                    FROM Kj2_Outbox
                    WHERE State IN (@Pending, @Failed, @Publishing)
                      AND AvailableAt <= clock_timestamp()
                    ORDER BY AvailableAt, CreatedAt
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING Id, Queue, EventType, PayloadJson, State,
                          PublishAttempts, AvailableAt, CreatedAt,
                          PublishedAt, LastError, ClaimToken;",
                new
                {
                    Pending = (int)OutboxDeliveryState.Pending,
                    Failed = (int)OutboxDeliveryState.Failed,
                    Publishing = (int)OutboxDeliveryState.Publishing,
                    ClaimDuration = claimDuration,
                    ClaimToken = claimToken
                },
                transaction,
                cancellationToken: cancellationToken));

            await transaction.CommitAsync(cancellationToken);

            if (claimed is null)
            {
                return null;
            }

            return new OutboxMessageRecord
            {
                Id = claimed.Id,
                Queue = claimed.Queue,
                EventType = claimed.EventType,
                PayloadJson = claimed.PayloadJson,
                State = OutboxDeliveryState.Publishing,
                PublishAttempts = claimed.PublishAttempts,
                AvailableAt = claimed.AvailableAt,
                CreatedAt = claimed.CreatedAt,
                PublishedAt = claimed.PublishedAt,
                LastError = claimed.LastError,
                ClaimToken = claimed.ClaimToken
            };
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private static async ValueTask MarkPublishedAsync(
        NpgsqlConnection connection,
        string messageId,
        string claimToken,
        CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
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
                    MessageId = messageId,
                    Published = (int)OutboxDeliveryState.Published,
                    Publishing = (int)OutboxDeliveryState.Publishing,
                    ClaimToken = claimToken
                },
                transaction,
                cancellationToken: cancellationToken));
            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    /// <summary>
    /// Best-effort failure mark on the shared connection. If the connection
    /// has already died we drop the row here; the publisher's next iteration
    /// will not see it stuck in Publishing because its AvailableAt will have
    /// elapsed and the publisher's claim-token check prevents double-marking.
    /// </summary>
    private static async ValueTask TryMarkDispatchFailedAsync(
        NpgsqlConnection connection,
        string messageId,
        string claimToken,
        string error,
        TimeSpan retryDelay)
    {
        try
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
        catch
        {
            // Connection is already broken; the row will be reclaimed after the
            // claim duration expires.
        }
    }

    /// <summary>
    /// Bounded abandon write so a slow database or pending shutdown can't
    /// stall the publisher. We open a fresh connection (the publisher's shared
    /// connection may already be in flight elsewhere) and race the write
    /// against a short timeout.
    /// </summary>
    private async ValueTask TryAbandonAsync(OutboxFailure failure)
    {
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await using var databasePermit = await AcquireDatabaseOperationAsync(timeoutCts.Token);
            await using var connection = await _backgroundDataSource.OpenConnectionAsync(timeoutCts.Token);
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
                    Abandoned = (int)OutboxDeliveryState.Abandoned,
                    Publishing = (int)OutboxDeliveryState.Publishing,
                    failure.MessageId,
                    failure.ClaimToken,
                    failure.Error
                },
                cancellationToken: timeoutCts.Token));
        }
        catch
        {
            // Connection or timeout failure: the row's claim token will expire
            // and another publisher iteration will surface it again.
        }
    }

    private sealed class OutboxClaimRow
    {
        public string Id { get; set; } = string.Empty;
        public string Queue { get; set; } = string.Empty;
        public string EventType { get; set; } = string.Empty;
        public string PayloadJson { get; set; } = string.Empty;
        public OutboxDeliveryState State { get; set; }
        public int PublishAttempts { get; set; }
        public DateTimeOffset AvailableAt { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset? PublishedAt { get; set; }
        public string? LastError { get; set; }
        public string? ClaimToken { get; set; }
    }
}
