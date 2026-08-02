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
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
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

        var target = command.DeliveryTarget
            ?? new DeliveryTarget(ExecutionDeliveryProfile.Pull, "default", null, "default");
        target.Validate();
        var runId = NewId();

        // INSERT ... RETURNING * removes the separate clock_timestamp() round
        // trip (CreatedAt is computed server-side) and returns the exact
        // persisted row, including the database clock's CreatedAt.
        var retryPolicyJson = command.RetryPolicy is not null
            ? JsonSerializer.Serialize(command.RetryPolicy, SerializerOptions)
            : null;
        var continuationJson = command.Continuation is not null
            ? JsonSerializer.Serialize(command.Continuation, SerializerOptions)
            : null;
        var compensationJson = command.Compensation is not null
            ? JsonSerializer.Serialize(command.Compensation, SerializerOptions)
            : null;

        var inserted = await connection.QuerySingleOrDefaultAsync<JobRunRecord>(new CommandDefinition(@"
            INSERT INTO Kj2_JobRuns
                (Id, JobKey, PayloadJson, Queue, ExecutionLane, DeliveryProfile, ConsumerGroup, TransportId, Priority, Phase, AvailableAt,
                 CreatedAt, AttemptCount, MaxAttempts, TimeoutSeconds, RetryPolicyJson,
                 ContinuationJson, CompensationJson,
                 IdempotencyKey, ConcurrencyKey, OrderingMode, CancelRequested, Version)
            VALUES
                (@Id, @JobKey, CAST(@PayloadJson AS jsonb), @Queue, @ExecutionLane, @DeliveryProfile, @ConsumerGroup, @TransportId, @Priority,
                 @Phase, @AvailableAt, clock_timestamp(), 0, @MaxAttempts,
                 @TimeoutSeconds, CAST(@RetryPolicyJson AS jsonb),
                 CAST(@ContinuationJson AS jsonb), CAST(@CompensationJson AS jsonb),
                 @IdempotencyKey, @ConcurrencyKey, @OrderingMode, FALSE, 0)
            ON CONFLICT (IdempotencyKey) DO NOTHING
            RETURNING *;",
            new
            {
                Id = runId,
                command.JobKey,
                command.PayloadJson,
                command.Queue,
                target.ExecutionLane,
                DeliveryProfile = (int)target.Profile,
                target.ConsumerGroup,
                target.TransportId,
                command.Priority,
                Phase = (int)JobPhase.Pending,
                AvailableAt = command.AvailableAt.ToUniversalTime(),
                command.MaxAttempts,
                command.TimeoutSeconds,
                RetryPolicyJson = retryPolicyJson,
                ContinuationJson = continuationJson,
                CompensationJson = compensationJson,
                command.IdempotencyKey,
                command.ConcurrencyKey,
                OrderingMode = (int)target.OrderingMode
            },
            transaction,
            cancellationToken: cancellationToken));

        if (inserted is null)
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
            inserted.Queue,
            OutboxEventTypes.WorkAvailable,
            JsonSerializer.Serialize(new { runId = inserted.Id, queue = inserted.Queue }, SerializerOptions),
            inserted.AvailableAt,
            cancellationToken,
            target,
            partitionKey: inserted.ConcurrencyKey);

        await transaction.CommitAsync(cancellationToken);
        return new SubmitJobResult(inserted, Existing: false);
    }

    public async ValueTask<IReadOnlyList<SubmitJobResult>> SubmitBatchAsync(
        IReadOnlyList<SubmitJobCommand> commands,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(commands);
        if (commands.Count == 0)
        {
            return Array.Empty<SubmitJobResult>();
        }

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var count = commands.Count;
        var ids = new string[count];
        var jobKeys = new string[count];
        var payloads = new string[count];
        var queues = new string[count];
        var executionLanes = new string[count];
        var deliveryProfiles = new int[count];
        var consumerGroups = new string[count];
        var transportIds = new string?[count];
        var priorities = new int[count];
        var availableAts = new DateTimeOffset[count];
        var maxAttempts = new int[count];
        var timeouts = new int[count];
        var retryPolicyJsons = new string?[count];
        var continuationJsons = new string?[count];
        var compensationJsons = new string?[count];
        var idempotencyKeys = new string?[count];
        var concurrencyKeys = new string?[count];
        var targets = new DeliveryTarget[count];

        for (var index = 0; index < count; index++)
        {
            var command = commands[index];
            var target = command.DeliveryTarget
                ?? new DeliveryTarget(ExecutionDeliveryProfile.Pull, "default", null, "default");
            target.Validate();
            ids[index] = NewId();
            jobKeys[index] = command.JobKey;
            payloads[index] = command.PayloadJson;
            queues[index] = command.Queue;
            executionLanes[index] = target.ExecutionLane;
            deliveryProfiles[index] = (int)target.Profile;
            consumerGroups[index] = target.ConsumerGroup;
            transportIds[index] = target.TransportId;
            priorities[index] = command.Priority;
            availableAts[index] = command.AvailableAt.ToUniversalTime();
            maxAttempts[index] = command.MaxAttempts;
            timeouts[index] = command.TimeoutSeconds;
            retryPolicyJsons[index] = command.RetryPolicy is not null
                ? JsonSerializer.Serialize(command.RetryPolicy, SerializerOptions) : null;
            continuationJsons[index] = command.Continuation is not null
                ? JsonSerializer.Serialize(command.Continuation, SerializerOptions) : null;
            compensationJsons[index] = command.Compensation is not null
                ? JsonSerializer.Serialize(command.Compensation, SerializerOptions) : null;
            idempotencyKeys[index] = command.IdempotencyKey;
            concurrencyKeys[index] = command.ConcurrencyKey;
            targets[index] = target;
        }

        // One multi-row INSERT with ON CONFLICT DO NOTHING. Rows without an
        // idempotency key never conflict (NULLs are distinct in the unique
        // constraint), so RETURNING yields exactly the newly inserted rows.
        // clock_timestamp() is inlined for CreatedAt, removing the per-row
        // now() round trip that SubmitAsync pays.
        var inserted = (await connection.QueryAsync<JobRunRecord>(new CommandDefinition(@"
            INSERT INTO Kj2_JobRuns
                (Id, JobKey, PayloadJson, Queue, ExecutionLane, DeliveryProfile, ConsumerGroup, TransportId, Priority, Phase, AvailableAt,
                 CreatedAt, AttemptCount, MaxAttempts, TimeoutSeconds, RetryPolicyJson,
                 ContinuationJson, CompensationJson,
                 IdempotencyKey, ConcurrencyKey, OrderingMode, CancelRequested, Version)
            SELECT
                item.Id, item.JobKey, CAST(item.PayloadJson AS jsonb), item.Queue, item.ExecutionLane, item.DeliveryProfile, item.ConsumerGroup,
                item.TransportId, item.Priority, @Pending, item.AvailableAt, clock_timestamp(), 0, item.MaxAttempts,
                item.TimeoutSeconds, CAST(item.RetryPolicyJson AS jsonb),
                CAST(item.ContinuationJson AS jsonb), CAST(item.CompensationJson AS jsonb),
                item.IdempotencyKey, item.ConcurrencyKey, item.OrderingMode, FALSE, 0
            FROM unnest(
                CAST(@Ids AS text[]),
                CAST(@JobKeys AS text[]),
                CAST(@Payloads AS text[]),
                CAST(@Queues AS text[]),
                CAST(@ExecutionLanes AS text[]),
                CAST(@DeliveryProfiles AS int[]),
                CAST(@ConsumerGroups AS text[]),
                CAST(@TransportIds AS text[]),
                CAST(@Priorities AS int[]),
                CAST(@AvailableAts AS timestamptz[]),
                CAST(@MaxAttempts AS int[]),
                CAST(@Timeouts AS int[]),
                CAST(@RetryPolicyJsons AS text[]),
                CAST(@ContinuationJsons AS text[]),
                CAST(@CompensationJsons AS text[]),
                CAST(@IdempotencyKeys AS text[]),
                CAST(@ConcurrencyKeys AS text[]),
                CAST(@OrderingModes AS int[]))
                AS item(Id, JobKey, PayloadJson, Queue, ExecutionLane, DeliveryProfile, ConsumerGroup, TransportId, Priority,
                        AvailableAt, MaxAttempts, TimeoutSeconds, RetryPolicyJson,
                        ContinuationJson, CompensationJson,
                        IdempotencyKey, ConcurrencyKey, OrderingMode)
            ON CONFLICT (IdempotencyKey) DO NOTHING
            RETURNING *;",
            new
            {
                Ids = ids,
                JobKeys = jobKeys,
                Payloads = payloads,
                Queues = queues,
                ExecutionLanes = executionLanes,
                DeliveryProfiles = deliveryProfiles,
                ConsumerGroups = consumerGroups,
                TransportIds = transportIds,
                Priorities = priorities,
                AvailableAts = availableAts,
                MaxAttempts = maxAttempts,
                Timeouts = timeouts,
                RetryPolicyJsons = retryPolicyJsons,
                ContinuationJsons = continuationJsons,
                CompensationJsons = compensationJsons,
                IdempotencyKeys = idempotencyKeys,
                ConcurrencyKeys = concurrencyKeys,
                OrderingModes = targets.Select(target => (int)target.OrderingMode).ToArray(),
                Pending = (int)JobPhase.Pending
            },
            transaction,
            cancellationToken: cancellationToken))).ToDictionary(x => x.Id, StringComparer.Ordinal);

        // Keyed commands that did not insert resolved to a pre-existing run; fetch
        // them in one round trip to run compatibility checks and return them.
        var missingKeys = new List<string>(count);
        for (var index = 0; index < count; index++)
        {
            if (!inserted.ContainsKey(ids[index])
                && !string.IsNullOrWhiteSpace(idempotencyKeys[index]))
            {
                missingKeys.Add(idempotencyKeys[index]!);
            }
        }

        var existingByKey = new Dictionary<string, JobRunRecord>(StringComparer.Ordinal);
        if (missingKeys.Count > 0)
        {
            var existing = await connection.QueryAsync<JobRunRecord>(new CommandDefinition(@"
                SELECT *
                FROM Kj2_JobRuns
                WHERE IdempotencyKey = ANY(@Keys);",
                new { Keys = missingKeys.Distinct().ToArray() },
                transaction,
                cancellationToken: cancellationToken));
            foreach (var run in existing)
            {
                existingByKey[run.IdempotencyKey!] = run;
            }
        }

        var results = new SubmitJobResult[count];
        var outboxItems = new List<(string Queue, string PayloadJson, DateTimeOffset AvailableAt, DeliveryTarget Target, string? PartitionKey)>(count);
        for (var index = 0; index < count; index++)
        {
            if (inserted.TryGetValue(ids[index], out var run))
            {
                results[index] = new SubmitJobResult(run, Existing: false);
                outboxItems.Add((
                    run.Queue,
                    JsonSerializer.Serialize(new { runId = run.Id, queue = run.Queue }, SerializerOptions),
                    run.AvailableAt,
                    targets[index],
                    run.ConcurrencyKey));
            }
            else
            {
                var key = idempotencyKeys[index]
                    ?? throw new InvalidOperationException(
                        "A non-idempotent KubeJob submission did not insert, which violates the batch submit contract.");
                if (!existingByKey.TryGetValue(key, out var existingRun))
                {
                    throw new InvalidOperationException(
                        $"KubeJob batch submit could not resolve an existing run for idempotency key '{key}'.");
                }

                JobSubmissionIdentity.EnsureCompatible(existingRun, commands[index]);
                results[index] = new SubmitJobResult(existingRun, Existing: true);
            }
        }

        if (outboxItems.Count > 0)
        {
            await AddOutboxBatchAsync(
                connection,
                transaction,
                outboxItems,
                OutboxEventTypes.WorkAvailable,
                cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return results;
    }

    public async ValueTask<bool> RequeueWorkAvailableAsync(
        string runId,
        DateTimeOffset availableAt,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var state = await connection.QuerySingleOrDefaultAsync<WorkRequeueState>(new CommandDefinition(@"
            SELECT Queue, ExecutionLane, DeliveryProfile, ConsumerGroup, TransportId, OrderingMode, Phase, CancelRequested, AvailableAt, ConcurrencyKey
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

        await AddOutboxAsync(
            connection,
            transaction,
            state.Queue,
            OutboxEventTypes.WorkAvailable,
            JsonSerializer.Serialize(new { runId, queue = state.Queue }, SerializerOptions),
            retryAt,
            cancellationToken,
            new DeliveryTarget(
                state.DeliveryProfile,
                state.ExecutionLane,
                state.TransportId,
                state.ConsumerGroup,
                state.OrderingMode),
            partitionKey: state.ConcurrencyKey);
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
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var state = await connection.QuerySingleOrDefaultAsync<RunCancelState>(new CommandDefinition(@"
            SELECT Id AS RunId,
                   Queue,
                   DeliveryProfile,
                   ConsumerGroup,
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
            && state.DeliveryProfile == ExecutionDeliveryProfile.BrokerDispatch
            && string.Equals(consumerGroup, state.ConsumerGroup, StringComparison.Ordinal)
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
        public string Queue { get; set; } = "default";
        public string ExecutionLane { get; set; } = "default";
        public ExecutionDeliveryProfile DeliveryProfile { get; set; } = ExecutionDeliveryProfile.Pull;
        public string ConsumerGroup { get; set; } = "default";
        public string? TransportId { get; set; }
        public ExecutionOrderingMode OrderingMode { get; set; } = ExecutionOrderingMode.Parallel;
        public string? ConcurrencyKey { get; set; }
    }

    public async ValueTask<JobRunRecord?> GetByIdempotencyKeyAsync(
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _businessDataSource.OpenConnectionAsync(cancellationToken);

        var run = await connection.QuerySingleOrDefaultAsync<JobRunRecord?>(new CommandDefinition(@"
            SELECT Id,
                   JobKey,
                   Queue,
                   Priority,
                   Phase,
                   PayloadJson,
                   CreatedAt,
                   StartedAt,
                   CompletedAt,
                   AttemptCount,
                   IdempotencyKey,
                   ConcurrencyKey,
                   OrderingMode,
                   DeliveryProfile,
                   ConsumerGroup,
                   TransportId
            FROM Kj2_JobRuns
            WHERE IdempotencyKey = @IdempotencyKey
              AND Phase NOT IN (@Succeeded, @Failed, @Canceled, @Dead)
            LIMIT 1",
            new
            {
                IdempotencyKey = idempotencyKey,
                Succeeded = (int)JobPhase.Succeeded,
                Failed = (int)JobPhase.Failed,
                Canceled = (int)JobPhase.Canceled,
                Dead = (int)JobPhase.Dead
            },
            cancellationToken: cancellationToken));

        return run;
    }

    private sealed class RunCancelState
    {
        public string RunId { get; set; } = string.Empty;
        public string Queue { get; set; } = string.Empty;
        public ExecutionDeliveryProfile DeliveryProfile { get; set; } = ExecutionDeliveryProfile.Pull;
        public string ConsumerGroup { get; set; } = "default";
        public int Phase { get; set; }
        public bool CancelRequested { get; set; }
    }
}
