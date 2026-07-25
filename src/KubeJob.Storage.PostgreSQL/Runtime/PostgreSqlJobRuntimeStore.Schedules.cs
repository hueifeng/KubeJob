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
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<JobScheduleRecord>(new CommandDefinition(@"
            INSERT INTO Kj2_JobSchedules
                (Id, JobKey, PayloadJson, CronExpression, TimeZoneId, Queue,
                 Priority, MisfirePolicy, ConcurrencyPolicy, MaxAttempts,
                 TimeoutSeconds, Enabled, NextFireAt, LastFireAt, ClaimToken,
                 ClaimUntil, CreatedAt, UpdatedAt, Version)
            VALUES
                (@Id, @JobKey, CAST(@PayloadJson AS jsonb), @CronExpression,
                 @TimeZoneId, @Queue, @Priority, @MisfirePolicy,
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
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var stored = await connection.QuerySingleAsync<JobScheduleRecord>(new CommandDefinition(@"
            INSERT INTO Kj2_JobSchedules
                (Id, JobKey, PayloadJson, CronExpression, TimeZoneId, Queue,
                 Priority, MisfirePolicy, ConcurrencyPolicy, MaxAttempts,
                 TimeoutSeconds, Enabled, NextFireAt, LastFireAt, ClaimToken,
                 ClaimUntil, CreatedAt, UpdatedAt, Version)
            VALUES
                (@Id, @JobKey, CAST(@PayloadJson AS jsonb), @CronExpression,
                 @TimeZoneId, @Queue, @Priority, @MisfirePolicy,
                 @ConcurrencyPolicy, @MaxAttempts, @TimeoutSeconds, @Enabled,
                 @NextFireAt, @LastFireAt, NULL, NULL, clock_timestamp(),
                 clock_timestamp(), 1)
            ON CONFLICT (Id) DO UPDATE SET
                JobKey = EXCLUDED.JobKey,
                PayloadJson = EXCLUDED.PayloadJson,
                CronExpression = EXCLUDED.CronExpression,
                TimeZoneId = EXCLUDED.TimeZoneId,
                Queue = EXCLUDED.Queue,
                Priority = EXCLUDED.Priority,
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
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
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
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var affected = await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_JobSchedules
            SET Enabled = @Enabled,
                NextFireAt = COALESCE(@NextFireAt, NextFireAt),
                ClaimToken = NULL,
                ClaimUntil = NULL,
                UpdatedAt = clock_timestamp(),
                Version = Version + 1
            WHERE Id = @ScheduleId;",
            new
            {
                ScheduleId = scheduleId,
                Enabled = enabled,
                NextFireAt = nextFireAt?.ToUniversalTime()
            },
            cancellationToken: cancellationToken));
        return affected > 0;
    }

    public async ValueTask<bool> DeleteAsync(
        string scheduleId,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteAsync(new CommandDefinition(
            "DELETE FROM Kj2_JobSchedules WHERE Id = @ScheduleId;",
            new { ScheduleId = scheduleId },
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

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
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

        var claims = new List<ClaimedSchedule>(schedules.Length);
        foreach (var schedule in schedules)
        {
            var claimToken = NewId();
            var version = schedule.Version + 1;
            await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_JobSchedules
                SET ClaimToken = @ClaimToken,
                    ClaimUntil = @ClaimUntil,
                    UpdatedAt = @Now,
                    Version = @Version
                WHERE Id = @ScheduleId
                  AND Version = @PreviousVersion;",
                new
                {
                    ScheduleId = schedule.Id,
                    ClaimToken = claimToken,
                    ClaimUntil = claimUntil,
                    Now = databaseNow,
                    Version = version,
                    PreviousVersion = schedule.Version
                },
                transaction,
                cancellationToken: cancellationToken));

            schedule.ClaimToken = claimToken;
            schedule.ClaimUntil = claimUntil;
            schedule.UpdatedAt = databaseNow;
            schedule.Version = version;
            claims.Add(new ClaimedSchedule(schedule, claimToken, version));
        }

        await transaction.CommitAsync(cancellationToken);
        return claims;
    }

    public async ValueTask<JobRunRecord?> CommitFireAsync(
        CommitScheduleFireCommand command,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
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
                    (Id, JobKey, PayloadJson, Queue, Priority, Phase, AvailableAt,
                     CreatedAt, AttemptCount, MaxAttempts, TimeoutSeconds,
                     IdempotencyKey, ScheduleId, ScheduledFor,
                     CancelRequested, Version)
                VALUES
                    (@Id, @JobKey, CAST(@PayloadJson AS jsonb), @Queue, @Priority,
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
                    run.Id,
                    run.Queue,
                    databaseNow,
                    cancellationToken);
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
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
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
