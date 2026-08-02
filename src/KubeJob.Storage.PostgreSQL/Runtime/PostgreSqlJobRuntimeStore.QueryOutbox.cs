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

    public async ValueTask<IReadOnlyList<JobRunRecord>> GetRunsAsync(
        IReadOnlyList<string> runIds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(runIds);
        if (runIds.Count == 0)
        {
            return Array.Empty<JobRunRecord>();
        }

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        return (await connection.QueryAsync<JobRunRecord>(new CommandDefinition(@"
            SELECT *
            FROM Kj2_JobRuns
            WHERE Id = ANY(@RunIds);",
            new { RunIds = runIds.ToArray() },
            cancellationToken: cancellationToken))).ToArray();
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

        // Collapse the previous two-commit cycle (claim transaction + flush
        // transaction) into a single transaction per batch. The rows are claimed
        // with FOR UPDATE SKIP LOCKED, the broker publish happens while the
        // transaction (and row locks) is still open, and the Published/Failed
        // markers are written in the same transaction before commit. Because the
        // rows are already claimed, other publishers SKIP LOCKED past them and
        // never block on this transaction. This halves the outbox write volume,
        // which is on the critical path for every ingested job.
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            var claimedBatch = await ClaimBatchAsync(connection, transaction, claimDuration, batchSize, cancellationToken);

            if (claimedBatch.Count == 0)
            {
                await transaction.CommitAsync(cancellationToken);
                return new OutboxDispatchBatch(dispatched, failed, abandoned);
            }

            // Dispatch the whole claimed batch concurrently so broker confirms
            // pipeline: each per-slot lock is held only for BasicPublish, not the
            // confirm wait, and the broker can batch its acks across the batch.
            // Per-message outcomes are inspected after Task.WhenAll so the shared
            // connection is still driven one command at a time during the mark
            // phase — only the transport publish (which owns its own connections)
            // runs concurrently.
            var dispatchTasks = claimedBatch
                .Select(claimed => DispatchToTaskAsync(claimed, dispatch, cancellationToken))
                .ToArray();

            try
            {
                await Task.WhenAll(dispatchTasks).ConfigureAwait(false);
            }
            catch
            {
                // Task.WhenAll rethrows the first failure, but every task is now
                // complete; classify each outcome below.
            }

            // Classify all outcomes first, then issue bulk UPDATEs
            // (one for Published, one for Failed+Abandoned) instead of N
            // sequential round-trips. For a batch of 256 this cuts ~256 DB
            // commands down to 2, which is the largest remaining outbox hot-path
            // cost after synchronous_commit=off.
            var publishedRows = new List<(string Id, string ClaimToken)>(claimedBatch.Count);
            var failedRows = new List<(string Id, string ClaimToken, string Error, OutboxDeliveryState TargetState)>(claimedBatch.Count);

            foreach (var (claimed, task) in claimedBatch.Zip(dispatchTasks, (c, t) => (c, t)))
            {
                if (task.IsCompletedSuccessfully)
                {
                    publishedRows.Add((claimed.Id, claimed.ClaimToken!));
                    dispatched.Add(claimed.Id);
                    continue;
                }

                var exception = task.IsFaulted
                    ? task.Exception!.GetBaseException()
                    : task.IsCanceled
                        ? new OperationCanceledException(cancellationToken)
                        : null;

                if (exception is PermanentOutboxException permanent)
                {
                    failedRows.Add((claimed.Id, claimed.ClaimToken!, permanent.Message, OutboxDeliveryState.Abandoned));
                    abandoned.Add(claimed.Id);
                }
                else
                {
                    var error = exception is OperationCanceledException
                        ? "publisher_canceled"
                        : exception?.Message ?? "publisher_canceled";
                    failedRows.Add((claimed.Id, claimed.ClaimToken!, error, OutboxDeliveryState.Failed));
                    failed.Add(claimed.Id);
                }
            }

            if (publishedRows.Count > 0)
            {
                await BulkMarkPublishedInTxAsync(connection, transaction, publishedRows, cancellationToken);
            }

            if (failedRows.Count > 0)
            {
                await BulkMarkFailedInTxAsync(connection, transaction, failedRows, retryDelay, cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);

            if (cancellationToken.IsCancellationRequested)
            {
                cancellationToken.ThrowIfCancellationRequested();
            }
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }

        return new OutboxDispatchBatch(dispatched, failed, abandoned);
    }

    /// <summary>
    /// Bulk-marks multiple claimed outbox rows as Published inside the caller's
    /// open transaction. Uses a single unnest UPDATE to collapse N per-message
    /// round-trips into one, which is the dominant outbox write cost when
    /// publishing large dispatch batches.
    /// </summary>
    private static async ValueTask BulkMarkPublishedInTxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<(string Id, string ClaimToken)> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0) return;

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_Outbox outbox
            SET State = @Published,
                PublishedAt = clock_timestamp(),
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
                Ids = rows.Select(r => r.Id).ToArray(),
                ClaimTokens = rows.Select(r => r.ClaimToken).ToArray()
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Bulk-marks multiple claimed outbox rows as Failed or Abandoned inside the
    /// caller's open transaction. Each row can carry its own error text and
    /// retry delay (zero for Abandoned). A single unnest UPDATE replaces N
    /// individual round-trips.
    /// </summary>
    private static async ValueTask BulkMarkFailedInTxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<(string Id, string ClaimToken, string Error, OutboxDeliveryState TargetState)> rows,
        TimeSpan retryDelay,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0) return;

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_Outbox outbox
            SET State = CAST(completed.TargetState AS integer),
                LastError = completed.Error,
                AvailableAt = CASE
                    WHEN completed.TargetStateNum = @AbandonedInt THEN clock_timestamp()
                    ELSE clock_timestamp() + @RetryInterval
                END,
                ClaimToken = NULL
            FROM unnest(
                CAST(@Ids AS text[]),
                CAST(@ClaimTokens AS text[]),
                CAST(@Errors AS text[]),
                CAST(@TargetStates AS integer[]))
                AS completed(Id, ClaimToken, Error, TargetStateNum)
            WHERE outbox.Id = completed.Id
              AND outbox.State = @Publishing
              AND outbox.ClaimToken = completed.ClaimToken;",
            new
            {
                Publishing = (int)OutboxDeliveryState.Publishing,
                AbandonedInt = (int)OutboxDeliveryState.Abandoned,
                RetryInterval = retryDelay,
                Ids = rows.Select(r => r.Id).ToArray(),
                ClaimTokens = rows.Select(r => r.ClaimToken).ToArray(),
                Errors = rows.Select(r => r.Error).ToArray(),
                TargetStates = rows.Select(r => (int)r.TargetState).ToArray()
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Invokes the per-message dispatch callback inside an async entry point so a
    /// synchronously thrown exception (e.g. <see cref="PermanentOutboxException"/>
    /// from an unknown event type) is captured into the returned <see cref="Task"/>
    /// instead of escaping the <see cref="Task.WhenAll(Task[])"/> setup. The
    /// caller inspects each task's outcome to classify it as published, failed, or
    /// abandoned.
    /// </summary>
    private static async Task DispatchToTaskAsync(
        OutboxMessageRecord claimed,
        Func<OutboxMessageRecord, CancellationToken, ValueTask> dispatch,
        CancellationToken cancellationToken)
    {
        await dispatch(claimed, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<IReadOnlyList<OutboxMessageRecord>> ClaimBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        TimeSpan claimDuration,
        int batchSize,
        CancellationToken cancellationToken)
    {
        // Single CTE: SELECT FOR UPDATE SKIP LOCKED → UPDATE (claim) → RETURNING.
        // Collapses the previous two round-trips (SELECT + separate UPDATE) into
        // one. For a batch of 512 rows at 50k TPS this eliminates ~512 round-trips
        // per dispatch cycle, which is the single largest remaining DB cost.
        //
        // The UPDATE must RETURN the claimed rows: a separate SELECT in the same
        // statement cannot see the UPDATE's effects (data-modifying CTEs run
        // under the statement snapshot), which silently returned an empty batch.
        // Tokens pair with rows by position (ordinality vs row_number), so each
        // claimed row keeps a unique claim token.
        var claimTokensRaw = new string[batchSize];
        for (var i = 0; i < batchSize; i++)
        {
            claimTokensRaw[i] = NewId();
        }

        var rows = (await connection.QueryAsync<OutboxMessageRecord>(new CommandDefinition(@"
            WITH raw AS (
                SELECT Id
                FROM Kj2_Outbox
                WHERE State IN (@Pending, @Failed, @Publishing)
                  AND AvailableAt <= clock_timestamp()
                ORDER BY AvailableAt, CreatedAt
                LIMIT @BatchSize
                FOR UPDATE SKIP LOCKED
            ),
            picked AS (
                SELECT Id, row_number() OVER () AS rn
                FROM raw
            ),
            claimed AS (
                UPDATE Kj2_Outbox outbox
                SET State = @Publishing,
                    PublishAttempts = outbox.PublishAttempts + 1,
                    AvailableAt = clock_timestamp() + @ClaimDuration,
                    ClaimToken = tokens.token
                FROM picked
                JOIN unnest(CAST(@ClaimTokens AS text[])) WITH ORDINALITY AS tokens(token, ord)
                  ON tokens.ord = picked.rn
                WHERE outbox.Id = picked.Id
                RETURNING outbox.*
            )
            SELECT * FROM claimed;",
            new
            {
                Pending = (int)OutboxDeliveryState.Pending,
                Failed = (int)OutboxDeliveryState.Failed,
                Publishing = (int)OutboxDeliveryState.Publishing,
                BatchSize = batchSize,
                ClaimDuration = claimDuration,
                ClaimTokens = claimTokensRaw
            },
            transaction,
            cancellationToken: cancellationToken))).ToArray();

        // Map claim tokens to the returned rows by matching ClaimToken.
        // The CTE returns rows in ClaimToken order (ANY with IN list preserves input order).
        for (var i = 0; i < rows.Length; i++)
        {
            rows[i].State = OutboxDeliveryState.Publishing;
        }

        return rows;
    }
}
