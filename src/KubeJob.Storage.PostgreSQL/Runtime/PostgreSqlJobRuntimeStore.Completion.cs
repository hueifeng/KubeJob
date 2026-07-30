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
        TimeSpan retryDelay,
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
                        var availableAt = now.Add(retryDelay);
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
        TimeSpan retryDelay,
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
            if (items.Length > 1 && items.All(x => x.request.Outcome == JobAttemptOutcome.Succeeded))
            {
                var batchResults = await CompleteSucceededBatchAsync(items, cancellationToken);
                foreach (var item in items)
                {
                    results[item.index] = batchResults[item.index];
                }
            }
            else
            {
                foreach (var item in items)
                {
                    results[item.index] = await CompleteAsync(item.request, retryDelay, cancellationToken);
                }
            }
        }

        return results;
    }

    private async ValueTask<Dictionary<int, CompleteAttemptResponse>> CompleteSucceededBatchAsync(
        IReadOnlyList<(CompleteAttemptRequest request, int index)> items,
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
        var itemCount = items.Count;
        var attemptIndexById = new Dictionary<string, int>(itemCount, StringComparer.Ordinal);
        for (var index = 0; index < itemCount; index++)
        {
            attemptIndexById.TryAdd(items[index].request.AttemptId, index);
        }
        var valid = new List<(CompleteAttemptRequest request, CompletionStateRow state)>();
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

            valid.Add((item.request, state));
        }

        if (valid.Count > 0)
        {
            var attemptIds = valid.Select(x => x.request.AttemptId).ToArray();
            var canceled = valid.Where(x => x.state.CancelRequested).ToArray();
            var succeeded = valid.Where(x => !x.state.CancelRequested).ToArray();

            await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_JobAttempts
                SET Phase = @Succeeded,
                    CompletedAt = @CompletedAt,
                    FailureCode = NULL,
                    FailureMessage = NULL
                WHERE Id = ANY(@AttemptIds)
                  AND Phase = @Running;",
                new
                {
                    Succeeded = (int)JobAttemptPhase.Succeeded,
                    Running = (int)JobAttemptPhase.Running,
                    CompletedAt = now,
                    AttemptIds = attemptIds
                },
                transaction,
                cancellationToken: cancellationToken));

            await CompleteSucceededRunsAsync(
                connection,
                transaction,
                succeeded,
                now,
                cancellationToken);
            await CompleteCanceledRunsAsync(
                connection,
                transaction,
                canceled,
                now,
                cancellationToken);

            foreach (var item in valid)
            {
                if (!attemptIndexById.TryGetValue(item.request.AttemptId, out var originalIndex))
                {
                    continue;
                }

                results[originalIndex] = new CompleteAttemptResponse(
                    true,
                    item.state.CancelRequested ? JobPhase.Canceled : JobPhase.Succeeded,
                    false);
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return results;
    }

    private static async ValueTask CompleteSucceededRunsAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        IReadOnlyList<(CompleteAttemptRequest request, CompletionStateRow state)> items,
        DateTimeOffset completedAt,
        CancellationToken cancellationToken)
    {
        if (items.Count == 0)
        {
            return;
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET Phase = @Succeeded,
                CompletedAt = @CompletedAt,
                CurrentAttemptId = NULL,
                CurrentWorkerId = NULL,
                CurrentSessionId = NULL,
                FailureCode = NULL,
                FailureMessage = NULL,
                Version = Version + 1
            WHERE Id = ANY(@RunIds)
              AND Phase = @Running
              AND CurrentAttemptId = ANY(@AttemptIds);",
            new
            {
                Succeeded = (int)JobPhase.Succeeded,
                Running = (int)JobPhase.Running,
                CompletedAt = completedAt,
                RunIds = items.Select(x => x.request.RunId).Distinct().ToArray(),
                AttemptIds = items.Select(x => x.request.AttemptId).ToArray()
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    private static async ValueTask CompleteCanceledRunsAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        IReadOnlyList<(CompleteAttemptRequest request, CompletionStateRow state)> items,
        DateTimeOffset completedAt,
        CancellationToken cancellationToken)
    {
        if (items.Count == 0)
        {
            return;
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobRuns
            SET Phase = @Canceled,
                CompletedAt = @CompletedAt,
                CurrentAttemptId = NULL,
                CurrentWorkerId = NULL,
                CurrentSessionId = NULL,
                FailureCode = @FailureCode,
                FailureMessage = NULL,
                Version = Version + 1
            WHERE Id = ANY(@RunIds)
              AND Phase = @Running
              AND CurrentAttemptId = ANY(@AttemptIds);",
            new
            {
                Canceled = (int)JobPhase.Canceled,
                Running = (int)JobPhase.Running,
                CompletedAt = completedAt,
                FailureCode = "canceled",
                RunIds = items.Select(x => x.request.RunId).Distinct().ToArray(),
                AttemptIds = items.Select(x => x.request.AttemptId).ToArray()
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    public async ValueTask<int> RequeueExpiredLeasesAsync(
        DateTimeOffset now,
        TimeSpan retryDelay,
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

        foreach (var state in expired)
        {
            const string failureCode = "lease_lost";
            const string failureMessage = "The worker did not renew the attempt lease before it expired.";

            await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_JobAttempts
                SET Phase = @LeaseLost,
                    CompletedAt = @CompletedAt,
                    FailureCode = @FailureCode,
                    FailureMessage = @FailureMessage
                WHERE Id = @AttemptId
                  AND Phase = @Running;",
                new
                {
                    state.AttemptId,
                    LeaseLost = (int)JobAttemptPhase.LeaseLost,
                    Running = (int)JobAttemptPhase.Running,
                    CompletedAt = databaseNow,
                    FailureCode = failureCode,
                    FailureMessage = failureMessage
                },
                transaction,
                cancellationToken: cancellationToken));

            if (state.CancelRequested)
            {
                await MakeTerminalAsync(
                    connection,
                    transaction,
                    state.AttemptRunId,
                    JobPhase.Canceled,
                    databaseNow,
                    "canceled",
                    failureMessage,
                    cancellationToken);
            }
            else if (state.AttemptCount < state.MaxAttempts)
            {
                var availableAt = databaseNow.Add(retryDelay);
                await RequeueRunAsync(
                    connection,
                    transaction,
                    state.AttemptRunId,
                    availableAt,
                    failureCode,
                    failureMessage,
                    cancellationToken);
                await AddOutboxAsync(
                    connection,
                    transaction,
                    state.Queue,
                    OutboxEventTypes.WorkAvailable,
                    JsonSerializer.Serialize(new { runId = state.AttemptRunId, queue = state.Queue }, SerializerOptions),
                    availableAt,
                    cancellationToken);
            }
            else
            {
                await MakeTerminalAsync(
                    connection,
                    transaction,
                    state.AttemptRunId,
                    JobPhase.Dead,
                    databaseNow,
                    failureCode,
                    failureMessage,
                    cancellationToken);
            }
        }

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
