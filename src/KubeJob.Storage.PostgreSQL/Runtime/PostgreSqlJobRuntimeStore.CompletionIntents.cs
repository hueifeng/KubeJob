using System.Text.Json;
using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore : ICompletionIntentFinalizer
{
    public async ValueTask<bool> PersistAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // Lock the current attempt/run first. This serializes completion intent
        // persistence against lease/timeout recovery and concurrent duplicate
        // completion requests without relying on hosted-service ordering.
        var state = await connection.QuerySingleOrDefaultAsync<CompletionReservationRow>(new CommandDefinition(@"
            SELECT attempt.Id AS AttemptId,
                   attempt.RunId AS AttemptRunId,
                   attempt.AttemptNumber,
                   attempt.WorkerId,
                   attempt.SessionId,
                   attempt.SessionEpoch,
                   attempt.LeaseToken,
                   attempt.FenceVersion AS AttemptFenceVersion,
                   attempt.LeaseExpiresAt,
                   attempt.StartedAt,
                   attempt.Phase AS AttemptPhase,
                   run.Phase AS RunPhase,
                   run.CurrentAttemptId,
                   run.CancelRequested,
                   run.FenceVersion AS RunFenceVersion
            FROM Kj2_JobAttempts attempt
            JOIN Kj2_JobRuns run ON run.Id = attempt.RunId
            WHERE attempt.Id = @AttemptId
              AND run.Id = @RunId
            FOR UPDATE OF attempt, run;",
            new { request.AttemptId, request.RunId },
            transaction,
            cancellationToken: cancellationToken));

        var existing = await connection.QuerySingleOrDefaultAsync<CompletionIntentRow>(new CommandDefinition(@"
            SELECT AttemptId,
                   RunId,
                   WorkerId,
                   SessionId,
                   SessionEpoch,
                   AttemptNumber,
                   LeaseToken,
                   FenceVersion,
                   Outcome,
                   FailureCode,
                   FailureMessage,
                   CreatedAt
            FROM Kj2_CompletionIntents
            WHERE AttemptId = @AttemptId
            FOR UPDATE;",
            new { request.AttemptId },
            transaction,
            cancellationToken: cancellationToken));

        if (existing is not null)
        {
            var sameIntent = CompletionIntentMatches(existing, request);
            await transaction.CommitAsync(cancellationToken);
            return sameIntent;
        }

        var sessionActive = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(@"
            SELECT EXISTS (
                SELECT 1
                FROM Kj2_WorkerSessions
                WHERE WorkerId = @WorkerId
                  AND SessionId = @SessionId
                  AND Epoch = @SessionEpoch
                  AND State IN (@Ready, @Draining)
                FOR UPDATE
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

        if (!sessionActive
            || state is null
            || state.AttemptPhase != JobAttemptPhase.Running
            || state.RunPhase != JobPhase.Running
            || state.CancelRequested
            || state.LeaseExpiresAt <= now
            || !string.Equals(state.AttemptRunId, request.RunId, StringComparison.Ordinal)
            || state.AttemptNumber != request.AttemptNumber
            || !string.Equals(state.WorkerId, request.WorkerId, StringComparison.Ordinal)
            || !string.Equals(state.SessionId, request.SessionId, StringComparison.Ordinal)
            || state.SessionEpoch != request.SessionEpoch
            || !string.Equals(state.LeaseToken, request.LeaseToken, StringComparison.Ordinal)
            || state.AttemptFenceVersion != request.FenceVersion
            || state.RunFenceVersion != request.FenceVersion
            || !string.Equals(state.CurrentAttemptId, request.AttemptId, StringComparison.Ordinal))
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            INSERT INTO Kj2_CompletionIntents
                (AttemptId, RunId, WorkerId, SessionId, SessionEpoch, AttemptNumber,
                 LeaseToken, FenceVersion, Outcome, FailureCode, FailureMessage, CreatedAt)
            VALUES
                (@AttemptId, @RunId, @WorkerId, @SessionId, @SessionEpoch, @AttemptNumber,
                 @LeaseToken, @FenceVersion, @Outcome, @FailureCode, @FailureMessage, @CreatedAt);",
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
                CreatedAt = now
            },
            transaction,
            cancellationToken: cancellationToken));

        var reserved = await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobAttempts
            SET Phase = @Completing
            WHERE Id = @AttemptId
              AND Phase = @Running
              AND FenceVersion = @FenceVersion;",
            new
            {
                request.AttemptId,
                request.FenceVersion,
                Completing = (int)JobAttemptPhase.Completing,
                Running = (int)JobAttemptPhase.Running
            },
            transaction,
            cancellationToken: cancellationToken));
        if (reserved != 1)
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        await transaction.CommitAsync(cancellationToken);
        return true;
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
            SELECT AttemptId,
                   RunId,
                   WorkerId,
                   SessionId,
                   SessionEpoch,
                   AttemptNumber,
                   LeaseToken,
                   FenceVersion,
                   Outcome,
                   FailureCode,
                   FailureMessage,
                   CreatedAt
            FROM Kj2_CompletionIntents
            ORDER BY CreatedAt
            LIMIT @BatchSize;",
            new { BatchSize = batchSize },
            cancellationToken: cancellationToken))).Select(ToRequest).ToArray();
    }

    public async ValueTask<CompleteAttemptResponse> FinalizeAsync(
        CompleteAttemptRequest request,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var state = await connection.QuerySingleOrDefaultAsync<PersistedCompletionStateRow>(new CommandDefinition(@"
            SELECT intent.AttemptId,
                   intent.RunId,
                   intent.WorkerId,
                   intent.SessionId,
                   intent.SessionEpoch,
                   intent.AttemptNumber,
                   intent.LeaseToken,
                   intent.FenceVersion,
                   intent.Outcome,
                   intent.FailureCode,
                   intent.FailureMessage,
                   intent.CreatedAt AS IntentCreatedAt,
                   attempt.StartedAt AS AttemptStartedAt,
                   attempt.LeaseExpiresAt,
                   attempt.FenceVersion AS AttemptFenceVersion,
                   attempt.Phase AS AttemptPhase,
                   run.Phase AS RunPhase,
                   run.CurrentAttemptId,
                   run.FenceVersion AS RunFenceVersion,
                   run.CancelRequested,
                   run.AttemptCount,
                   run.MaxAttempts,
                   run.Queue,
                   run.ExecutionLane,
                   run.DeliveryProfile,
                   run.ConsumerGroup,
                   run.TransportId,
                   run.OrderingMode,
                   run.ConcurrencyKey,
                   run.RetryPolicyJson,
                   run.Priority,
                   run.TimeoutSeconds,
                   run.ContinuationJson,
                   run.CompensationJson
            FROM Kj2_CompletionIntents intent
            JOIN Kj2_JobAttempts attempt ON attempt.Id = intent.AttemptId
            JOIN Kj2_JobRuns run ON run.Id = attempt.RunId
            WHERE intent.AttemptId = @AttemptId
              AND intent.RunId = @RunId
            FOR UPDATE OF intent, attempt, run;",
            new { request.AttemptId, request.RunId },
            transaction,
            cancellationToken: cancellationToken));

        if (state is null
            || !CompletionIntentMatches(state, request)
            || state.AttemptPhase != JobAttemptPhase.Completing
            || state.RunPhase != JobPhase.Running
            || state.AttemptFenceVersion != state.FenceVersion
            || state.RunFenceVersion != state.FenceVersion
            || !string.Equals(state.CurrentAttemptId, state.AttemptId, StringComparison.Ordinal))
        {
            await transaction.RollbackAsync(cancellationToken);
            return new CompleteAttemptResponse(
                false,
                state?.RunPhase ?? JobPhase.Failed,
                false,
                "stale_or_conflicting_completion_intent");
        }

        var timedOut = state.IntentCreatedAt >= state.AttemptStartedAt.AddSeconds(state.TimeoutSeconds);
        var effectiveOutcome = timedOut ? JobAttemptOutcome.TimedOut : state.Outcome;
        var effectiveFailureCode = timedOut ? "timeout" : state.FailureCode;
        var effectiveFailureMessage = timedOut
            ? $"Execution exceeded its {state.TimeoutSeconds} second timeout before completion was accepted."
            : state.FailureMessage;
        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));

        var updated = await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobAttempts
            SET Phase = @Phase,
                CompletedAt = @CompletedAt,
                FailureCode = @FailureCode,
                FailureMessage = @FailureMessage
            WHERE Id = @AttemptId
              AND Phase = @Completing
              AND FenceVersion = @FenceVersion;",
            new
            {
                state.AttemptId,
                state.FenceVersion,
                Phase = (int)MapAttemptPhase(effectiveOutcome),
                CompletedAt = now,
                FailureCode = effectiveFailureCode,
                FailureMessage = effectiveFailureMessage,
                Completing = (int)JobAttemptPhase.Completing
            },
            transaction,
            cancellationToken: cancellationToken));
        if (updated != 1)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new CompleteAttemptResponse(false, state.RunPhase, false, "completion_intent_lost_ownership");
        }

        var completionState = state.ToCompletionState();
        JobPhase phase;
        var requeued = false;

        if (state.CancelRequested || effectiveOutcome == JobAttemptOutcome.Canceled)
        {
            phase = JobPhase.Canceled;
            await MakeTerminalAsync(
                connection,
                transaction,
                state.RunId,
                phase,
                now,
                effectiveFailureCode ?? "canceled",
                effectiveFailureMessage,
                cancellationToken);
        }
        else
        {
            switch (effectiveOutcome)
            {
                case JobAttemptOutcome.Succeeded:
                    phase = JobPhase.Succeeded;
                    await MakeTerminalAsync(
                        connection, transaction, state.RunId, phase, now, null, null, cancellationToken);
                    await FireTerminalActionsAsync(
                        connection, transaction, completionState, effectiveOutcome, now, cancellationToken);
                    break;

                case JobAttemptOutcome.PermanentFailure:
                    phase = JobPhase.Failed;
                    await MakeTerminalAsync(
                        connection, transaction, state.RunId, phase, now,
                        effectiveFailureCode, effectiveFailureMessage, cancellationToken);
                    await FireTerminalActionsAsync(
                        connection, transaction, completionState, effectiveOutcome, now, cancellationToken);
                    break;

                case JobAttemptOutcome.RetryableFailure:
                case JobAttemptOutcome.TimedOut:
                    if (state.AttemptCount < state.MaxAttempts)
                    {
                        phase = JobPhase.Pending;
                        requeued = true;
                        var effectivePolicy = ResolveRetryPolicy(completionState, retryPolicy);
                        var availableAt = now.Add(effectivePolicy.ComputeDelay(state.AttemptCount));
                        await RequeueRunAsync(
                            connection,
                            transaction,
                            state.RunId,
                            availableAt,
                            effectiveFailureCode,
                            effectiveFailureMessage,
                            cancellationToken);
                        await AddOutboxAsync(
                            connection,
                            transaction,
                            state.Queue,
                            OutboxEventTypes.WorkAvailable,
                            JsonSerializer.Serialize(new { runId = state.RunId, queue = state.Queue }, SerializerOptions),
                            availableAt,
                            cancellationToken,
                            new DeliveryTarget(
                                state.DeliveryProfile,
                                state.ExecutionLane,
                                state.TransportId,
                                state.ConsumerGroup,
                                state.OrderingMode),
                            partitionKey: state.ConcurrencyKey);
                    }
                    else
                    {
                        phase = JobPhase.Dead;
                        await MakeTerminalAsync(
                            connection, transaction, state.RunId, phase, now,
                            effectiveFailureCode, effectiveFailureMessage, cancellationToken);
                        await FireTerminalActionsAsync(
                            connection, transaction, completionState, effectiveOutcome, now, cancellationToken);
                    }
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(effectiveOutcome), effectiveOutcome, null);
            }
        }

        await DeleteCompletionIntentsAsync(
            connection,
            transaction,
            new[] { state.AttemptId },
            cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new CompleteAttemptResponse(true, phase, requeued);
    }

    public async ValueTask<IReadOnlyList<CompleteAttemptResponse>> FinalizeBatchAsync(
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
        for (var index = 0; index < requests.Count; index++)
        {
            results[index] = await FinalizeAsync(requests[index], retryPolicy, cancellationToken);
        }

        return results;
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

    private static CompleteAttemptRequest ToRequest(CompletionIntentRow row) => new(
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
        row.FenceVersion);

    private static bool CompletionIntentMatches(
        CompletionIntentRow row,
        CompleteAttemptRequest request) =>
        string.Equals(row.AttemptId, request.AttemptId, StringComparison.Ordinal)
        && string.Equals(row.RunId, request.RunId, StringComparison.Ordinal)
        && string.Equals(row.WorkerId, request.WorkerId, StringComparison.Ordinal)
        && string.Equals(row.SessionId, request.SessionId, StringComparison.Ordinal)
        && row.SessionEpoch == request.SessionEpoch
        && row.AttemptNumber == request.AttemptNumber
        && string.Equals(row.LeaseToken, request.LeaseToken, StringComparison.Ordinal)
        && row.FenceVersion == request.FenceVersion
        && row.Outcome == request.Outcome
        && string.Equals(row.FailureCode, request.FailureCode, StringComparison.Ordinal)
        && string.Equals(row.FailureMessage, request.FailureMessage, StringComparison.Ordinal);

    private sealed class CompletionReservationRow
    {
        public string AttemptId { get; set; } = string.Empty;
        public string AttemptRunId { get; set; } = string.Empty;
        public int AttemptNumber { get; set; }
        public string WorkerId { get; set; } = string.Empty;
        public string SessionId { get; set; } = string.Empty;
        public long SessionEpoch { get; set; }
        public string LeaseToken { get; set; } = string.Empty;
        public long AttemptFenceVersion { get; set; }
        public DateTimeOffset LeaseExpiresAt { get; set; }
        public DateTimeOffset StartedAt { get; set; }
        public JobAttemptPhase AttemptPhase { get; set; }
        public JobPhase RunPhase { get; set; }
        public string? CurrentAttemptId { get; set; }
        public bool CancelRequested { get; set; }
        public long RunFenceVersion { get; set; }
    }

    private class CompletionIntentRow
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
        public DateTimeOffset CreatedAt { get; set; }
    }

    private sealed class PersistedCompletionStateRow : CompletionIntentRow
    {
        public DateTimeOffset IntentCreatedAt { get; set; }
        public DateTimeOffset AttemptStartedAt { get; set; }
        public DateTimeOffset LeaseExpiresAt { get; set; }
        public long AttemptFenceVersion { get; set; }
        public JobAttemptPhase AttemptPhase { get; set; }
        public JobPhase RunPhase { get; set; }
        public string? CurrentAttemptId { get; set; }
        public long RunFenceVersion { get; set; }
        public bool CancelRequested { get; set; }
        public int AttemptCount { get; set; }
        public int MaxAttempts { get; set; }
        public string Queue { get; set; } = "default";
        public string ExecutionLane { get; set; } = "default";
        public ExecutionDeliveryProfile DeliveryProfile { get; set; } = ExecutionDeliveryProfile.Pull;
        public string ConsumerGroup { get; set; } = "default";
        public string? TransportId { get; set; }
        public ExecutionOrderingMode OrderingMode { get; set; } = ExecutionOrderingMode.Parallel;
        public string? ConcurrencyKey { get; set; }
        public string? RetryPolicyJson { get; set; }
        public int Priority { get; set; }
        public int TimeoutSeconds { get; set; }
        public string? ContinuationJson { get; set; }
        public string? CompensationJson { get; set; }

        public CompletionStateRow ToCompletionState() => new()
        {
            AttemptId = AttemptId,
            AttemptRunId = RunId,
            AttemptNumber = AttemptNumber,
            WorkerId = WorkerId,
            SessionId = SessionId,
            SessionEpoch = SessionEpoch,
            LeaseToken = LeaseToken,
            AttemptFenceVersion = AttemptFenceVersion,
            LeaseExpiresAt = LeaseExpiresAt,
            AttemptPhase = AttemptPhase,
            RunPhase = RunPhase,
            CurrentAttemptId = CurrentAttemptId,
            RunFenceVersion = RunFenceVersion,
            CancelRequested = CancelRequested,
            AttemptCount = AttemptCount,
            MaxAttempts = MaxAttempts,
            Queue = Queue,
            ExecutionLane = ExecutionLane,
            DeliveryProfile = DeliveryProfile,
            ConsumerGroup = ConsumerGroup,
            TransportId = TransportId,
            OrderingMode = OrderingMode,
            ConcurrencyKey = ConcurrencyKey,
            RetryPolicyJson = RetryPolicyJson,
            Priority = Priority,
            TimeoutSeconds = TimeoutSeconds,
            ContinuationJson = ContinuationJson,
            CompensationJson = CompensationJson
        };
    }
}
