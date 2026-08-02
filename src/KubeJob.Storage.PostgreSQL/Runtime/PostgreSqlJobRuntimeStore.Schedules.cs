using System.Text.Json;
using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<JobScheduleRecord?> CreateIfAbsentAsync(
        JobScheduleRecord schedule,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<JobScheduleRecord>(new CommandDefinition(@"
            INSERT INTO Kj2_JobSchedules
                (Id, JobKey, PayloadJson, CronExpression, TimeZoneId, Queue,
                 ExecutionLane, DeliveryProfile, ConsumerGroup, TransportId, OrderingMode,
                 Priority, MisfirePolicy, ConcurrencyPolicy, MaxAttempts,
                 TimeoutSeconds, Enabled, NextFireAt, LastFireAt, ClaimToken,
                 ClaimUntil, CreatedAt, UpdatedAt, Version)
            VALUES
                (@Id, @JobKey, CAST(@PayloadJson AS jsonb), @CronExpression,
                 @TimeZoneId, @Queue, @ExecutionLane, @DeliveryProfile, @ConsumerGroup, @TransportId, @OrderingMode,
                 @Priority, @MisfirePolicy,
                 @ConcurrencyPolicy, @MaxAttempts, @TimeoutSeconds, @Enabled,
                 @NextFireAt, @LastFireAt, NULL, NULL, clock_timestamp(),
                 clock_timestamp(), 1)
            ON CONFLICT (Id) DO NOTHING
            RETURNING *;",
            new
            {
                schedule.Id,
                schedule.JobKey,
                schedule.PayloadJson,
                schedule.CronExpression,
                schedule.TimeZoneId,
                schedule.Queue,
                schedule.ExecutionLane,
                DeliveryProfile = (int)schedule.DeliveryProfile,
                schedule.ConsumerGroup,
                schedule.TransportId,
                OrderingMode = (int)schedule.OrderingMode,
                schedule.Priority,
                MisfirePolicy = (int)schedule.MisfirePolicy,
                ConcurrencyPolicy = (int)schedule.ConcurrencyPolicy,
                schedule.MaxAttempts,
                schedule.TimeoutSeconds,
                schedule.Enabled,
                NextFireAt = schedule.NextFireAt.ToUniversalTime(),
                LastFireAt = schedule.LastFireAt?.ToUniversalTime()
            },
            cancellationToken: cancellationToken));
    }

    public async ValueTask<JobScheduleRecord> UpsertAsync(
        JobScheduleRecord schedule,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        var stored = await connection.QuerySingleAsync<JobScheduleRecord>(new CommandDefinition(@"
            INSERT INTO Kj2_JobSchedules
                (Id, JobKey, PayloadJson, CronExpression, TimeZoneId, Queue,
                 ExecutionLane, DeliveryProfile, ConsumerGroup, TransportId, OrderingMode,
                 Priority, MisfirePolicy, ConcurrencyPolicy, MaxAttempts,
                 TimeoutSeconds, Enabled, NextFireAt, LastFireAt, ClaimToken,
                 ClaimUntil, CreatedAt, UpdatedAt, Version)
            VALUES
                (@Id, @JobKey, CAST(@PayloadJson AS jsonb), @CronExpression,
                 @TimeZoneId, @Queue, @ExecutionLane, @DeliveryProfile, @ConsumerGroup, @TransportId, @OrderingMode,
                 @Priority, @MisfirePolicy,
                 @ConcurrencyPolicy, @MaxAttempts, @TimeoutSeconds, @Enabled,
                 @NextFireAt, @LastFireAt, NULL, NULL, clock_timestamp(),
                 clock_timestamp(), 1)
            ON CONFLICT (Id) DO UPDATE SET
                JobKey = EXCLUDED.JobKey,
                PayloadJson = EXCLUDED.PayloadJson,
                CronExpression = EXCLUDED.CronExpression,
                TimeZoneId = EXCLUDED.TimeZoneId,
                Queue = EXCLUDED.Queue,
                ExecutionLane = EXCLUDED.ExecutionLane,
                DeliveryProfile = EXCLUDED.DeliveryProfile,
                ConsumerGroup = EXCLUDED.ConsumerGroup,
                TransportId = EXCLUDED.TransportId,
                OrderingMode = EXCLUDED.OrderingMode,
                MisfirePolicy = EXCLUDED.MisfirePolicy,
                ConcurrencyPolicy = EXCLUDED.ConcurrencyPolicy,
                MaxAttempts = EXCLUDED.MaxAttempts,
                TimeoutSeconds = EXCLUDED.TimeoutSeconds,
                Enabled = EXCLUDED.Enabled,
                NextFireAt = EXCLUDED.NextFireAt,
                ClaimToken = NULL,
                ClaimUntil = NULL,
                UpdatedAt = clock_timestamp(),
                Version = Kj2_JobSchedules.Version + 1
            RETURNING *;",
            new
            {
                schedule.Id,
                schedule.JobKey,
                schedule.PayloadJson,
                schedule.CronExpression,
                schedule.TimeZoneId,
                schedule.Queue,
                schedule.ExecutionLane,
                DeliveryProfile = (int)schedule.DeliveryProfile,
                schedule.ConsumerGroup,
                schedule.TransportId,
                OrderingMode = (int)schedule.OrderingMode,
                schedule.Priority,
                MisfirePolicy = (int)schedule.MisfirePolicy,
                ConcurrencyPolicy = (int)schedule.ConcurrencyPolicy,
                schedule.MaxAttempts,
                schedule.TimeoutSeconds,
                schedule.Enabled,
                NextFireAt = schedule.NextFireAt.ToUniversalTime(),
                LastFireAt = schedule.LastFireAt?.ToUniversalTime()
            },
            cancellationToken: cancellationToken));
        return stored;
    }

    public async ValueTask<JobScheduleRecord?> GetAsync(
        string scheduleId,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<JobScheduleRecord>(new CommandDefinition(@"
            SELECT *
            FROM Kj2_JobSchedules
            WHERE Id = @ScheduleId
            LIMIT 1;",
            new { ScheduleId = scheduleId },
            cancellationToken: cancellationToken));
    }

    public async ValueTask<bool> SetEnabledAsync(
        string scheduleId,
        bool enabled,
        DateTimeOffset? nextFireAt,
        long? expectedVersion,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        var affected = await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobSchedules
            SET Enabled = @Enabled,
                NextFireAt = COALESCE(@NextFireAt, NextFireAt),
                ClaimToken = NULL,
                ClaimUntil = NULL,
                UpdatedAt = clock_timestamp(),
                Version = Version + 1
            WHERE Id = @ScheduleId
              AND (@ExpectedVersion IS NULL OR Version = @ExpectedVersion);",
            new
            {
                ScheduleId = scheduleId,
                Enabled = enabled,
                NextFireAt = nextFireAt?.ToUniversalTime(),
                ExpectedVersion = expectedVersion
            },
            cancellationToken: cancellationToken));
        return affected > 0;
    }

    public async ValueTask<bool> DeleteAsync(
        string scheduleId,
        long? expectedVersion,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteAsync(new CommandDefinition(
            "DELETE FROM Kj2_JobSchedules WHERE Id = @ScheduleId AND (@ExpectedVersion IS NULL OR Version = @ExpectedVersion);",
            new { ScheduleId = scheduleId, ExpectedVersion = expectedVersion },
            cancellationToken: cancellationToken)) > 0;
    }

    public async ValueTask<IReadOnlyList<ClaimedSchedule>> ClaimDueAsync(
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
            return Array.Empty<ClaimedSchedule>();
        }

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var databaseNow = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));
        var claimUntil = databaseNow.Add(claimDuration);

        var schedules = (await connection.QueryAsync<JobScheduleRecord>(new CommandDefinition(@"
            SELECT *
            FROM Kj2_JobSchedules
            WHERE Enabled = TRUE
              AND NextFireAt <= @Now
              AND (ClaimUntil IS NULL OR ClaimUntil <= @Now)
            ORDER BY NextFireAt, Id
            FOR UPDATE SKIP LOCKED
            LIMIT @BatchSize;",
            new { Now = databaseNow, BatchSize = batchSize },
            transaction,
            cancellationToken: cancellationToken))).ToArray();

        if (schedules.Length == 0)
        {
            await transaction.CommitAsync(cancellationToken);
            return Array.Empty<ClaimedSchedule>();
        }

        var claimTokens = schedules.Select(_ => NewId()).ToArray();
        var previousVersions = schedules.Select(schedule => schedule.Version).ToArray();
        var nextVersions = previousVersions.Select(version => version + 1).ToArray();

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobSchedules schedule
            SET ClaimToken = claimed.ClaimToken,
                ClaimUntil = @ClaimUntil,
                UpdatedAt = @Now,
                Version = claimed.NextVersion
            FROM unnest(
                CAST(@Ids AS text[]),
                CAST(@ClaimTokens AS text[]),
                CAST(@NextVersions AS bigint[]))
                AS claimed(Id, ClaimToken, NextVersion)
            WHERE schedule.Id = claimed.Id;",
            new
            {
                ClaimUntil = claimUntil,
                Now = databaseNow,
                Ids = schedules.Select(schedule => schedule.Id).ToArray(),
                ClaimTokens = claimTokens,
                NextVersions = nextVersions
            },
            transaction,
            cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);

        var claims = new List<ClaimedSchedule>(schedules.Length);
        for (var index = 0; index < schedules.Length; index++)
        {
            var schedule = schedules[index];
            schedule.ClaimToken = claimTokens[index];
            schedule.ClaimUntil = claimUntil;
            schedule.UpdatedAt = databaseNow;
            schedule.Version = nextVersions[index];
            claims.Add(new ClaimedSchedule(schedule, claimTokens[index], nextVersions[index]));
        }

        return claims;
    }

    public async ValueTask<JobRunRecord?> CommitFireAsync(
        CommitScheduleFireCommand command,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var schedule = await connection.QuerySingleOrDefaultAsync<JobScheduleRecord>(new CommandDefinition(@"
            SELECT *
            FROM Kj2_JobSchedules
            WHERE Id = @ScheduleId
              AND ClaimToken = @ClaimToken
              AND Version = @ExpectedVersion
              AND ClaimUntil > clock_timestamp()
            FOR UPDATE;",
            new
            {
                command.ScheduleId,
                command.ClaimToken,
                command.ExpectedVersion
            },
            transaction,
            cancellationToken: cancellationToken));

        if (schedule is null)
        {
            await transaction.RollbackAsync(cancellationToken);
            return null;
        }

        var createRun = command.CreateRun;
        if (createRun && schedule.ConcurrencyPolicy == ScheduleConcurrencyPolicy.SkipIfRunning)
        {
            createRun = !await connection.ExecuteScalarAsync<bool>(new CommandDefinition(@"
                SELECT EXISTS (
                    SELECT 1
                    FROM Kj2_JobRuns
                    WHERE ScheduleId = @ScheduleId
                      AND Phase IN (@Pending, @Running)
                );",
                new
                {
                    command.ScheduleId,
                    Pending = (int)JobPhase.Pending,
                    Running = (int)JobPhase.Running
                },
                transaction,
                cancellationToken: cancellationToken));
        }

        var databaseNow = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));
        JobRunRecord? run = null;

        if (createRun)
        {
            var inserted = await connection.ExecuteAsync(new CommandDefinition(@"
                INSERT INTO Kj2_JobRuns
                    (Id, JobKey, PayloadJson, Queue, ExecutionLane, DeliveryProfile, ConsumerGroup, TransportId, OrderingMode,
                     Priority, Phase, AvailableAt,
                     CreatedAt, AttemptCount, MaxAttempts, TimeoutSeconds,
                     IdempotencyKey, ScheduleId, ScheduledFor,
                     CancelRequested, Version)
                VALUES
                    (@Id, @JobKey, CAST(@PayloadJson AS jsonb), @Queue, @ExecutionLane, @DeliveryProfile, @ConsumerGroup, @TransportId, @OrderingMode,
                     @Priority,
                     @Pending, @Now, @Now, 0, @MaxAttempts, @TimeoutSeconds,
                     @IdempotencyKey, @ScheduleId, @ScheduledFor, FALSE, 0)
                ON CONFLICT (ScheduleId, ScheduledFor)
                    WHERE ScheduleId IS NOT NULL AND ScheduledFor IS NOT NULL
                DO NOTHING;",
                new
                {
                    Id = command.RunId,
                    schedule.JobKey,
                    schedule.PayloadJson,
                    schedule.Queue,
                    schedule.ExecutionLane,
                    DeliveryProfile = (int)schedule.DeliveryProfile,
                    schedule.ConsumerGroup,
                    schedule.TransportId,
                    OrderingMode = (int)schedule.OrderingMode,
                    schedule.Priority,
                    Pending = (int)JobPhase.Pending,
                    Now = databaseNow,
                    schedule.MaxAttempts,
                    schedule.TimeoutSeconds,
                    command.IdempotencyKey,
                    ScheduleId = schedule.Id,
                    ScheduledFor = command.ScheduledFor.ToUniversalTime()
                },
                transaction,
                cancellationToken: cancellationToken));

            run = await connection.QuerySingleOrDefaultAsync<JobRunRecord>(new CommandDefinition(@"
                SELECT *
                FROM Kj2_JobRuns
                WHERE ScheduleId = @ScheduleId
                  AND ScheduledFor = @ScheduledFor
                LIMIT 1;",
                new
                {
                    ScheduleId = schedule.Id,
                    ScheduledFor = command.ScheduledFor.ToUniversalTime()
                },
                transaction,
                cancellationToken: cancellationToken));

            if (inserted > 0 && run is not null)
            {
                await AddOutboxAsync(
                    connection,
                    transaction,
                    run.Queue,
                    OutboxEventTypes.WorkAvailable,
                    JsonSerializer.Serialize(new { runId = run.Id, queue = run.Queue }, SerializerOptions),
                    databaseNow,
                    cancellationToken,
                    new DeliveryTarget(
                        schedule.DeliveryProfile,
                        schedule.ExecutionLane,
                        schedule.TransportId,
                        schedule.ConsumerGroup,
                        schedule.OrderingMode),
                    partitionKey: run.ConcurrencyKey);
            }
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobSchedules
            SET NextFireAt = @NextFireAt,
                LastFireAt = CASE WHEN @CreatedRun THEN @ScheduledFor ELSE LastFireAt END,
                ClaimToken = NULL,
                ClaimUntil = NULL,
                UpdatedAt = @Now,
                Version = Version + 1
            WHERE Id = @ScheduleId
              AND ClaimToken = @ClaimToken
              AND Version = @ExpectedVersion;",
            new
            {
                command.ScheduleId,
                command.ClaimToken,
                command.ExpectedVersion,
                NextFireAt = command.NextFireAt.ToUniversalTime(),
                CreatedRun = run is not null,
                ScheduledFor = command.ScheduledFor.ToUniversalTime(),
                Now = databaseNow
            },
            transaction,
            cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return run;
    }

    public async ValueTask ReleaseClaimAsync(
        string scheduleId,
        string claimToken,
        DateTimeOffset retryAt,
        CancellationToken cancellationToken)
    {
        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _backgroundDataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobSchedules
            SET ClaimToken = NULL,
                ClaimUntil = @RetryAt,
                UpdatedAt = clock_timestamp(),
                Version = Version + 1
            WHERE Id = @ScheduleId
              AND ClaimToken = @ClaimToken;",
            new
            {
                ScheduleId = scheduleId,
                ClaimToken = claimToken,
                RetryAt = retryAt.ToUniversalTime()
            },
            cancellationToken: cancellationToken));
    }
}
