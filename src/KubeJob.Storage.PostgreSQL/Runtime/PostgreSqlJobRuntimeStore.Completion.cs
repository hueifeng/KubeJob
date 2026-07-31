using System.Data;
using System.Text.Json;
using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // Registration uses the same lock. Whichever transaction commits first defines
        // whether this session is still current when the completion is evaluated.
        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT pg_advisory_xact_lock(hashtext(@WorkerId));",
            new { request.WorkerId },
            transaction,
            cancellationToken: cancellationToken));

        var sessionActive = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(@"
            SELECT EXISTS (
                SELECT 1
                FROM Kj2_WorkerSessions
                WHERE WorkerId = @WorkerId
                  AND SessionId = @SessionId
                  AND Epoch = @SessionEpoch
                  AND State IN (@Ready, @Draining)
            );",
            new
            {
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                Ready = (int)WorkerSessionState.Ready,
                Draining = (int)WorkerSessionState.Draining
            },
            transaction,
            cancellationToken: cancellationToken));

        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));

        var state = await connection.QuerySingleOrDefaultAsync<CompletionStateRow>(new CommandDefinition(@"
            SELECT
                attempt.Id AS AttemptId,
                attempt.RunId AS AttemptRunId,
                attempt.AttemptNumber,
                attempt.WorkerId,
                attempt.SessionId,
                attempt.SessionEpoch,
                attempt.LeaseToken,
                attempt.LeaseExpiresAt,
                attempt.Phase AS AttemptPhase,
                run.Phase AS RunPhase,
                run.CurrentAttemptId,
                run.CancelRequested,
                run.AttemptCount,
                run.MaxAttempts,
                run.Queue
            FROM Kj2_JobAttempts attempt
            JOIN Kj2_JobRuns run ON run.Id = attempt.RunId
            WHERE attempt.Id = @AttemptId
              AND run.Id = @RunId
            FOR UPDATE OF attempt, run;",
            new { request.AttemptId, request.RunId },
            transaction,
            cancellationToken: cancellationToken));

        if (!sessionActive || state is null || !MatchesFence(state, request, now))
        {
            await transaction.RollbackAsync(cancellationToken);
            return new CompleteAttemptResponse(
                false,
                state?.RunPhase ?? JobPhase.Failed,
                false,
                "stale_session_attempt_expired_or_fencing_token_mismatch");
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobAttempts
            SET Phase = @Phase,
                CompletedAt = @CompletedAt,
                FailureCode = @FailureCode,
                FailureMessage = @FailureMessage
            WHERE Id = @AttemptId
              AND Phase = @Running;",
            new
            {
                request.AttemptId,
                Phase = (int)MapAttemptPhase(request.Outcome),
                CompletedAt = now,
                request.FailureCode,
                request.FailureMessage,
                Running = (int)JobAttemptPhase.Running
            },
            transaction,
            cancellationToken: cancellationToken));

        JobPhase phase;
        var requeued = false;

        if (state.CancelRequested || request.Outcome == JobAttemptOutcome.Canceled)
        {
            phase = JobPhase.Canceled;
            await MakeTerminalAsync(
                connection,
                transaction,
                request.RunId,
                phase,
                now,
                request.FailureCode ?? "canceled",
                request.FailureMessage,
                cancellationToken);
        }
        else
        {
            switch (request.Outcome)
            {
                case JobAttemptOutcome.Succeeded:
                    phase = JobPhase.Succeeded;
                    await MakeTerminalAsync(
                        connection,
                        transaction,
                        request.RunId,
                        phase,
                        now,
                        null,
                        null,
                        cancellationToken);
                    break;

                case JobAttemptOutcome.PermanentFailure:
                    phase = JobPhase.Failed;
                    await MakeTerminalAsync(
                        connection,
                        transaction,
                        request.RunId,
                        phase,
                        now,
                        request.FailureCode,
                        request.FailureMessage,
                        cancellationToken);
                    break;

                case JobAttemptOutcome.RetryableFailure:
                case JobAttemptOutcome.TimedOut:
                    if (state.AttemptCount < state.MaxAttempts)
                    {
                        phase = JobPhase.Pending;
                        requeued = true;
                        var availableAt = now.Add(retryPolicy.ComputeDelay(state.AttemptCount));
                        await RequeueRunAsync(
                            connection,
                            transaction,
                            request.RunId,
                            availableAt,
                            request.FailureCode,
                            request.FailureMessage,
                            cancellationToken);
                        await AddOutboxAsync(
                            connection,
                            transaction,
                            state.Queue,
                            OutboxEventTypes.WorkAvailable,
                            JsonSerializer.Serialize(new { runId = request.RunId, queue = state.Queue }, SerializerOptions),
                            availableAt,
                            cancellationToken);
                    }
                    else
                    {
                        phase = JobPhase.Dead;
                        await MakeTerminalAsync(
                            connection,
                            transaction,
                            request.RunId,
                            phase,
                            now,
                            request.FailureCode,
                            request.FailureMessage,
                            cancellationToken);
                    }
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(request.Outcome), request.Outcome, null);
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return new CompleteAttemptResponse(true, phase, requeued);
    }

    public async ValueTask<IReadOnlyList<CompleteAttemptResponse>> CompleteBatchAsync(
        IReadOnlyList<CompleteAttemptRequest> requests,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(requests);
        if (requests.Count == 0)
        {
            return Array.Empty<CompleteAttemptResponse>();
        }

        var results = new CompleteAttemptResponse[requests.Count];
        var groups = requests
            .Select((request, index) => (request, index))
            .GroupBy(x => (x.request.WorkerId, x.request.SessionId, x.request.SessionEpoch));

        foreach (var group in groups)
        {
            var items = group.ToArray();
            if (items.Length > 1)
            {
                var batchResults = await CompleteGroupAsync(items, retryPolicy, cancellationToken);
                foreach (var item in items)
                {
                    results[item.index] = batchResults[item.index];
                }
            }
            else
            {
                foreach (var item in items)
                {
                    results[item.index] = await CompleteAsync(item.request, retryPolicy, cancellationToken);
                }
            }
        }

        return results;
    }

    private async ValueTask<Dictionary<int, CompleteAttemptResponse>> CompleteGroupAsync(
        IReadOnlyList<(CompleteAttemptRequest request, int index)> items,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken)
    {
        var first = items[0].request;
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT pg_advisory_xact_lock(hashtext(@WorkerId));",
            new { first.WorkerId },
            transaction,
            cancellationToken: cancellationToken));

        var sessionActive = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(@"
            SELECT EXISTS (
                SELECT 1
                FROM Kj2_WorkerSessions
                WHERE WorkerId = @WorkerId
                  AND SessionId = @SessionId
                  AND Epoch = @SessionEpoch
                  AND State IN (@Ready, @Draining)
            );",
            new
            {
                first.WorkerId,
                first.SessionId,
                first.SessionEpoch,
                Ready = (int)WorkerSessionState.Ready,
                Draining = (int)WorkerSessionState.Draining
            },
            transaction,
            cancellationToken: cancellationToken));

        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));

        var states = (await connection.QueryAsync<CompletionStateRow>(new CommandDefinition(@"
            SELECT
                attempt.Id AS AttemptId,
                attempt.RunId AS AttemptRunId,
                attempt.AttemptNumber,
                attempt.WorkerId,
                attempt.SessionId,
                attempt.SessionEpoch,
                attempt.LeaseToken,
                attempt.LeaseExpiresAt,
                attempt.Phase AS AttemptPhase,
                run.Phase AS RunPhase,
                run.CurrentAttemptId,
                run.CancelRequested,
                run.AttemptCount,
                run.MaxAttempts,
                run.Queue
            FROM Kj2_JobAttempts attempt
            JOIN Kj2_JobRuns run ON run.Id = attempt.RunId
            WHERE attempt.Id = ANY(@AttemptIds)
            FOR UPDATE OF attempt, run;",
            new { AttemptIds = items.Select(x => x.request.AttemptId).Distinct().ToArray() },
            transaction,
            cancellationToken: cancellationToken))).ToDictionary(x => x.AttemptId, StringComparer.Ordinal);

        var results = new Dictionary<int, CompleteAttemptResponse>();
        var seenAttempts = new HashSet<string>(StringComparer.Ordinal);
        var valid = new List<(CompleteAttemptRequest request, int index, CompletionStateRow state)>();
        foreach (var item in items)
        {
            var state = states.GetValueOrDefault(item.request.AttemptId);
            if (!sessionActive
                || state is null
                || !seenAttempts.Add(item.request.AttemptId)
                || !MatchesFence(state, item.request, now))
            {
                results[item.index] = new CompleteAttemptResponse(
                    false,
                    state?.RunPhase ?? JobPhase.Failed,
                    false,
                    "stale_session_attempt_expired_or_fencing_token_mismatch");
                continue;
            }

            valid.Add((item.request, item.index, state));
        }

        if (valid.Count > 0)
        {
            await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_JobAttempts
                SET Phase = item.Phase::smallint,
                    CompletedAt = @CompletedAt,
                    FailureCode = item.FailureCode,
                    FailureMessage = item.FailureMessage
                FROM unnest(
                    CAST(@AttemptIds AS text[]),
                    CAST(@Phases AS smallint[]),
                    CAST(@FailureCodes AS text[]),
                    CAST(@FailureMessages AS text[]))
                    AS item(AttemptId, Phase, FailureCode, FailureMessage)
                WHERE Kj2_JobAttempts.Id = item.AttemptId
                  AND Kj2_JobAttempts.Phase = @Running;",
                new
                {
                    AttemptIds = valid.Select(x => x.request.AttemptId).ToArray(),
                    Phases = valid.Select(x => (short)MapAttemptPhase(x.request.Outcome)).ToArray(),
                    FailureCodes = valid.Select(x => x.request.FailureCode).ToArray(),
                    FailureMessages = valid.Select(x => x.request.FailureMessage).ToArray(),
                    CompletedAt = now,
                    Running = (int)JobAttemptPhase.Running
                },
                transaction,
                cancellationToken: cancellationToken));

            var canceled = new List<(string RunId, string AttemptId, string? FailureCode, string? FailureMessage)>();
            var succeeded = new List<(string RunId, string AttemptId, string? FailureCode, string? FailureMessage)>();
            var failed = new List<(string RunId, string AttemptId, string? FailureCode, string? FailureMessage)>();
            var retryable = new List<(string RunId, string AttemptId, string? FailureCode, string? FailureMessage, DateTimeOffset AvailableAt)>();
            var dead = new List<(string RunId, string AttemptId, string? FailureCode, string? FailureMessage)>();

            foreach (var (request, index, state) in valid)
            {
                if (state.CancelRequested || request.Outcome == JobAttemptOutcome.Canceled)
                {
                    canceled.Add((request.RunId, request.AttemptId, request.FailureCode ?? "canceled", request.FailureMessage));
                    results[index] = new CompleteAttemptResponse(true, JobPhase.Canceled, false);
                    continue;
                }

                switch (request.Outcome)
                {
                    case JobAttemptOutcome.Succeeded:
                        succeeded.Add((request.RunId, request.AttemptId, null, null));
                        results[index] = new CompleteAttemptResponse(true, JobPhase.Succeeded, false);
                        break;

                    case JobAttemptOutcome.PermanentFailure:
                        failed.Add((request.RunId, request.AttemptId, request.FailureCode, request.FailureMessage));
                        results[index] = new CompleteAttemptResponse(true, JobPhase.Failed, false);
                        break;

                    case JobAttemptOutcome.RetryableFailure:
                    case JobAttemptOutcome.TimedOut:
                        if (state.AttemptCount < state.MaxAttempts)
                        {
                            var availableAt = now.Add(retryPolicy.ComputeDelay(state.AttemptCount));
                            retryable.Add((request.RunId, request.AttemptId, request.FailureCode, request.FailureMessage, availableAt));
                            results[index] = new CompleteAttemptResponse(true, JobPhase.Pending, true);
                        }
                        else
                        {
                            dead.Add((request.RunId, request.AttemptId, request.FailureCode, request.FailureMessage));
                            results[index] = new CompleteAttemptResponse(true, JobPhase.Dead, false);
                        }

                        break;

                    default:
                        throw new ArgumentOutOfRangeException(nameof(request.Outcome), request.Outcome, null);
                }
            }

            await SetTerminalRunBatchAsync(connection, transaction, succeeded, JobPhase.Succeeded, now, cancellationToken);
            await SetTerminalRunBatchAsync(connection, transaction, canceled, JobPhase.Canceled, now, cancellationToken);
            await SetTerminalRunBatchAsync(connection, transaction, failed, JobPhase.Failed, now, cancellationToken);
            await SetTerminalRunBatchAsync(connection, transaction, dead, JobPhase.Dead, now, cancellationToken);

            if (retryable.Count > 0)
            {
                await RequeueRunBatchWithReasonsAsync(connection, transaction, retryable, cancellationToken);
                var stateByRunId = valid.ToDictionary(x => x.request.RunId, x => x.state.Queue, StringComparer.Ordinal);
                await AddOutboxBatchAsync(
                    connection,
                    transaction,
                    retryable
                        .Select(x => (
                            Queue: stateByRunId[x.RunId],
                            Payload: JsonSerializer.Serialize(new { runId = x.RunId, queue = stateByRunId[x.RunId] }, SerializerOptions),
                            x.AvailableAt))
                        .ToArray(),
                    OutboxEventTypes.WorkAvailable,
                    cancellationToken);
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return results;
    }

    private static async ValueTask SetTerminalRunBatchAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        IReadOnlyList<(string RunId, string AttemptId, string? FailureCode, string? FailureMessage)> items,
        JobPhase phase,
        DateTimeOffset completedAt,
        CancellationToken cancellationToken)
    {
        if (items.Count == 0)
        {
            return;
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET Phase = @Phase,
                CompletedAt = @CompletedAt,
                CurrentAttemptId = NULL,
                CurrentWorkerId = NULL,
                CurrentSessionId = NULL,
                FailureCode = item.FailureCode,
                FailureMessage = item.FailureMessage,
                Version = Version + 1
            FROM unnest(
                CAST(@RunIds AS text[]),
                CAST(@AttemptIds AS text[]),
                CAST(@FailureCodes AS text[]),
                CAST(@FailureMessages AS text[]))
                AS item(RunId, AttemptId, FailureCode, FailureMessage)
            WHERE Kj2_JobRuns.Id = item.RunId
              AND Kj2_JobRuns.Phase = @Running
              AND Kj2_JobRuns.CurrentAttemptId = item.AttemptId;",
            new
            {
                RunIds = items.Select(x => x.RunId).ToArray(),
                AttemptIds = items.Select(x => x.AttemptId).ToArray(),
                FailureCodes = items.Select(x => x.FailureCode).ToArray(),
                FailureMessages = items.Select(x => x.FailureMessage).ToArray(),
                Phase = (int)phase,
                CompletedAt = completedAt,
                Running = (int)JobPhase.Running
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    private static async ValueTask RequeueRunBatchWithReasonsAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        IReadOnlyList<(string RunId, string AttemptId, string? FailureCode, string? FailureMessage, DateTimeOffset AvailableAt)> items,
        CancellationToken cancellationToken)
    {
        if (items.Count == 0)
        {
            return;
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET Phase = @Pending,
                AvailableAt = item.AvailableAt,
                CurrentAttemptId = NULL,
                CurrentWorkerId = NULL,
                CurrentSessionId = NULL,
                FailureCode = item.FailureCode,
                FailureMessage = item.FailureMessage,
                Version = Version + 1
            FROM unnest(
                CAST(@RunIds AS text[]),
                CAST(@AttemptIds AS text[]),
                CAST(@FailureCodes AS text[]),
                CAST(@FailureMessages AS text[]),
                CAST(@AvailableAts AS timestamptz[]))
                AS item(RunId, AttemptId, FailureCode, FailureMessage, AvailableAt)
            WHERE Kj2_JobRuns.Id = item.RunId
              AND Kj2_JobRuns.Phase = @Running
              AND Kj2_JobRuns.CurrentAttemptId = item.AttemptId;",
            new
            {
                RunIds = items.Select(x => x.RunId).ToArray(),
                AttemptIds = items.Select(x => x.AttemptId).ToArray(),
                FailureCodes = items.Select(x => x.FailureCode).ToArray(),
                FailureMessages = items.Select(x => x.FailureMessage).ToArray(),
                AvailableAts = items.Select(x => x.AvailableAt.ToUniversalTime()).ToArray(),
                Pending = (int)JobPhase.Pending,
                Running = (int)JobPhase.Running
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    public async ValueTask<int> RequeueExpiredLeasesAsync(
        DateTimeOffset now,
        RetryPolicy retryPolicy,
        int batchSize,
        CancellationToken cancellationToken)
    {
        _ = now;
        if (batchSize <= 0)
        {
            return 0;
        }

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var databaseNow = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));

        var expired = (await connection.QueryAsync<CompletionStateRow>(new CommandDefinition(@"
            SELECT
                attempt.Id AS AttemptId,
                attempt.RunId AS AttemptRunId,
                attempt.AttemptNumber,
                attempt.WorkerId,
                attempt.SessionId,
                attempt.SessionEpoch,
                attempt.LeaseToken,
                attempt.LeaseExpiresAt,
                attempt.Phase AS AttemptPhase,
                run.Phase AS RunPhase,
                run.CurrentAttemptId,
                run.CancelRequested,
                run.AttemptCount,
                run.MaxAttempts,
                run.Queue
            FROM Kj2_JobAttempts attempt
            JOIN Kj2_JobRuns run ON run.Id = attempt.RunId
            WHERE attempt.Phase = @AttemptRunning
              AND attempt.LeaseExpiresAt <= @Now
              AND run.Phase = @RunRunning
              AND run.CurrentAttemptId = attempt.Id
            ORDER BY attempt.LeaseExpiresAt
            FOR UPDATE OF attempt, run SKIP LOCKED
            LIMIT @BatchSize;",
            new
            {
                AttemptRunning = (int)JobAttemptPhase.Running,
                RunRunning = (int)JobPhase.Running,
                Now = databaseNow,
                BatchSize = batchSize
            },
            transaction,
            cancellationToken: cancellationToken))).ToArray();

        if (expired.Length == 0)
        {
            await transaction.CommitAsync(cancellationToken);
            return 0;
        }

        const string failureCode = "lease_lost";
        const string failureMessage = "The worker did not renew the attempt lease before it expired.";

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobAttempts
            SET Phase = @LeaseLost,
                CompletedAt = @CompletedAt,
                FailureCode = @FailureCode,
                FailureMessage = @FailureMessage
            WHERE Id = ANY(@AttemptIds)
              AND Phase = @Running;",
            new
            {
                AttemptIds = expired.Select(x => x.AttemptId).ToArray(),
                LeaseLost = (int)JobAttemptPhase.LeaseLost,
                Running = (int)JobAttemptPhase.Running,
                CompletedAt = databaseNow,
                FailureCode = failureCode,
                FailureMessage = failureMessage
            },
            transaction,
            cancellationToken: cancellationToken));

        var canceled = expired.Where(x => x.CancelRequested).ToArray();
        var retryable = expired.Where(x => !x.CancelRequested && x.AttemptCount < x.MaxAttempts).ToArray();
        var dead = expired.Where(x => !x.CancelRequested && x.AttemptCount >= x.MaxAttempts).ToArray();

        await MakeTerminalBatchAsync(
            connection,
            transaction,
            canceled.Select(x => x.AttemptRunId).ToArray(),
            JobPhase.Canceled,
            databaseNow,
            "canceled",
            failureMessage,
            cancellationToken);

        if (retryable.Length > 0)
        {
            var retryItems = retryable
                .Select(x => (x.AttemptRunId, AvailableAt: databaseNow.Add(retryPolicy.ComputeDelay(x.AttemptCount))))
                .ToArray();
            await RequeueRunBatchAsync(
                connection,
                transaction,
                retryItems,
                failureCode,
                failureMessage,
                cancellationToken);
            var availableAtByRunId = retryItems.ToDictionary(x => x.AttemptRunId, x => x.AvailableAt, StringComparer.Ordinal);
            await AddOutboxBatchAsync(
                connection,
                transaction,
                retryable
                    .Select(x => (
                        x.Queue,
                        Payload: JsonSerializer.Serialize(new { runId = x.AttemptRunId, queue = x.Queue }, SerializerOptions),
                        AvailableAt: availableAtByRunId[x.AttemptRunId]))
                    .ToArray(),
                OutboxEventTypes.WorkAvailable,
                cancellationToken);
        }

        await MakeTerminalBatchAsync(
            connection,
            transaction,
            dead.Select(x => x.AttemptRunId).ToArray(),
            JobPhase.Dead,
            databaseNow,
            failureCode,
            failureMessage,
            cancellationToken);

        await transaction.CommitAsync(cancellationToken);
        return expired.Length;
    }

    private static bool MatchesFence(
        CompletionStateRow state,
        CompleteAttemptRequest request,
        DateTimeOffset now) =>
        state.AttemptPhase == JobAttemptPhase.Running
        && state.RunPhase == JobPhase.Running
        && state.LeaseExpiresAt > now
        && string.Equals(state.AttemptRunId, request.RunId, StringComparison.Ordinal)
        && state.AttemptNumber == request.AttemptNumber
        && string.Equals(state.WorkerId, request.WorkerId, StringComparison.Ordinal)
        && string.Equals(state.SessionId, request.SessionId, StringComparison.Ordinal)
        && state.SessionEpoch == request.SessionEpoch
        && string.Equals(state.LeaseToken, request.LeaseToken, StringComparison.Ordinal)
        && string.Equals(state.CurrentAttemptId, request.AttemptId, StringComparison.Ordinal);

    private static async ValueTask MakeTerminalAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        string runId,
        JobPhase phase,
        DateTimeOffset completedAt,
        string? failureCode,
        string? failureMessage,
        CancellationToken cancellationToken)
    {
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET Phase = @Phase,
                CompletedAt = @CompletedAt,
                CurrentAttemptId = NULL,
                CurrentWorkerId = NULL,
                CurrentSessionId = NULL,
                FailureCode = @FailureCode,
                FailureMessage = @FailureMessage,
                Version = Version + 1
            WHERE Id = @RunId;",
            new
            {
                RunId = runId,
                Phase = (int)phase,
                CompletedAt = completedAt,
                FailureCode = failureCode,
                FailureMessage = failureMessage
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    private static async ValueTask MakeTerminalBatchAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        IReadOnlyList<string> runIds,
        JobPhase phase,
        DateTimeOffset completedAt,
        string? failureCode,
        string? failureMessage,
        CancellationToken cancellationToken)
    {
        if (runIds.Count == 0)
        {
            return;
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET Phase = @Phase,
                CompletedAt = @CompletedAt,
                CurrentAttemptId = NULL,
                CurrentWorkerId = NULL,
                CurrentSessionId = NULL,
                FailureCode = @FailureCode,
                FailureMessage = @FailureMessage,
                Version = Version + 1
            WHERE Id = ANY(@RunIds);",
            new
            {
                RunIds = runIds.ToArray(),
                Phase = (int)phase,
                CompletedAt = completedAt,
                FailureCode = failureCode,
                FailureMessage = failureMessage
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    private static async ValueTask RequeueRunBatchAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        IReadOnlyList<(string RunId, DateTimeOffset AvailableAt)> items,
        string? failureCode,
        string? failureMessage,
        CancellationToken cancellationToken)
    {
        if (items.Count == 0)
        {
            return;
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET Phase = @Pending,
                AvailableAt = item.AvailableAt,
                CurrentAttemptId = NULL,
                CurrentWorkerId = NULL,
                CurrentSessionId = NULL,
                FailureCode = @FailureCode,
                FailureMessage = @FailureMessage,
                Version = Version + 1
            FROM unnest(CAST(@RunIds AS text[]), CAST(@AvailableAts AS timestamptz[]))
                AS item(RunId, AvailableAt)
            WHERE Kj2_JobRuns.Id = item.RunId;",
            new
            {
                RunIds = items.Select(x => x.RunId).ToArray(),
                AvailableAts = items.Select(x => x.AvailableAt.ToUniversalTime()).ToArray(),
                Pending = (int)JobPhase.Pending,
                FailureCode = failureCode,
                FailureMessage = failureMessage
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    private static async ValueTask AddOutboxBatchAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        IReadOnlyList<(string Queue, string PayloadJson, DateTimeOffset AvailableAt)> items,
        string eventType,
        CancellationToken cancellationToken)
    {
        if (items.Count == 0)
        {
            return;
        }

        var target = new DeliveryTarget(ExecutionDeliveryProfile.Pull, "default", null);
        target.Validate();

        await connection.ExecuteAsync(new CommandDefinition(@"
            INSERT INTO Kj2_Outbox
                (Id, Queue, DeliveryProfile, ExecutionLane, TransportId, EventType, PayloadJson, State, PublishAttempts,
                 AvailableAt, CreatedAt)
            SELECT
                item.Id, item.Queue, @DeliveryProfile, @ExecutionLane, @TransportId, @EventType,
                CAST(item.PayloadJson AS jsonb), @State, 0,
                GREATEST(item.AvailableAt, clock_timestamp()), clock_timestamp()
            FROM unnest(
                CAST(@Ids AS text[]),
                CAST(@Queues AS text[]),
                CAST(@Payloads AS text[]),
                CAST(@AvailableAts AS timestamptz[]))
                AS item(Id, Queue, PayloadJson, AvailableAt);",
            new
            {
                Ids = items.Select(_ => NewId()).ToArray(),
                Queues = items.Select(x => x.Queue).ToArray(),
                Payloads = items.Select(x => x.PayloadJson).ToArray(),
                AvailableAts = items.Select(x => x.AvailableAt.ToUniversalTime()).ToArray(),
                DeliveryProfile = (int)target.Profile,
                target.ExecutionLane,
                target.TransportId,
                EventType = eventType,
                State = (int)OutboxDeliveryState.Pending
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    private static async ValueTask RequeueRunAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        string runId,
        DateTimeOffset availableAt,
        string? failureCode,
        string? failureMessage,
        CancellationToken cancellationToken)
    {
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET Phase = @Pending,
                AvailableAt = @AvailableAt,
                CurrentAttemptId = NULL,
                CurrentWorkerId = NULL,
                CurrentSessionId = NULL,
                FailureCode = @FailureCode,
                FailureMessage = @FailureMessage,
                Version = Version + 1
            WHERE Id = @RunId;",
            new
            {
                RunId = runId,
                Pending = (int)JobPhase.Pending,
                AvailableAt = availableAt,
                FailureCode = failureCode,
                FailureMessage = failureMessage
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    private sealed class CompletionStateRow
    {
        public string AttemptId { get; set; } = string.Empty;
        public string AttemptRunId { get; set; } = string.Empty;
        public int AttemptNumber { get; set; }
        public string WorkerId { get; set; } = string.Empty;
        public string SessionId { get; set; } = string.Empty;
        public long SessionEpoch { get; set; }
        public string LeaseToken { get; set; } = string.Empty;
        public DateTimeOffset LeaseExpiresAt { get; set; }
        public JobAttemptPhase AttemptPhase { get; set; }
        public JobPhase RunPhase { get; set; }
        public string? CurrentAttemptId { get; set; }
        public bool CancelRequested { get; set; }
        public int AttemptCount { get; set; }
        public int MaxAttempts { get; set; }
        public string Queue { get; set; } = "default";
    }
}
