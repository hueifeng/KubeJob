using System.Data;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Cronos;
using Dapper;
using KubeJob.Core.Enums;
using KubeJob.Server.Data;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Data;

/// <summary>
/// Claims one due spec per short transaction. Cursor advancement and deterministic run insertion
/// commit together, so Server failover cannot create a half-materialized schedule.
/// </summary>
public sealed class PostgreSqlScheduleMaterializer : IKubeJobScheduleMaterializer
{
    private const int MaxSpecsPerTick = 256;
    private const int MaxShards = 4096;
    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlScheduleMaterializer(NpgsqlDataSource dataSource) => _dataSource = dataSource;

    public async Task<ScheduleMaterializationResult> MaterializeDueSchedulesAsync(
        int limit, CancellationToken cancellationToken)
    {
        var processed = 0;
        var inserted = 0;
        var skipped = 0;
        var emptyBroadcasts = 0;
        var invalid = 0;
        var bounded = Math.Clamp(limit, 1, MaxSpecsPerTick);

        for (var index = 0; index < bounded; index++)
        {
            var result = await MaterializeOneAsync(cancellationToken);
            if (!result.Found) break;
            processed++;
            inserted += result.Inserted;
            skipped += result.Skipped ? 1 : 0;
            emptyBroadcasts += result.EmptyBroadcast ? 1 : 0;
            invalid += result.Invalid ? 1 : 0;
        }
        return new ScheduleMaterializationResult(processed, inserted, skipped, emptyBroadcasts, invalid);
    }

    private async Task<OneResult> MaterializeOneAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp()", transaction: transaction, cancellationToken: cancellationToken));
        const string claimSql = """
            SELECT Id, JobType, CronExpression,
                   COALESCE(NodeSelector,'{}'::jsonb)::text AS NodeSelectorJson,
                   ExecuteModel, GREATEST(1,TotalShards) AS TotalShards, NextRunTime,
                   TimeoutSeconds, MaxRetries, ConcurrencyPolicy, QueueName, Priority,
                   RequiredHandlerVersion, PayloadSchemaVersion, ScheduleTimeZone
            FROM Kj_JobSpecs
            WHERE IsDisabled=FALSE AND CronExpression IS NOT NULL AND CronExpression<>''
              AND (NextRunTime IS NULL OR NextRunTime<=@Now)
            ORDER BY NextRunTime NULLS FIRST, Id
            FOR UPDATE SKIP LOCKED LIMIT 1;
            """;
        var spec = await connection.QuerySingleOrDefaultAsync<DueSpec>(new CommandDefinition(
            claimSql, new { Now = now }, transaction, cancellationToken: cancellationToken));
        if (spec is null)
        {
            await transaction.CommitAsync(cancellationToken);
            return default;
        }

        DateTimeOffset next;
        try
        {
            var cron = CronExpression.Parse(spec.CronExpression, CronFormat.Standard);
            var zone = string.IsNullOrWhiteSpace(spec.ScheduleTimeZone) ||
                       string.Equals(spec.ScheduleTimeZone, "UTC", StringComparison.OrdinalIgnoreCase)
                ? TimeZoneInfo.Utc : TimeZoneInfo.FindSystemTimeZoneById(spec.ScheduleTimeZone);
            next = cron.GetNextOccurrence(now, zone, inclusive: false)?.ToUniversalTime()
                ?? throw new InvalidOperationException("Cron has no reachable next occurrence.");
        }
        catch (Exception ex) when (ex is CronFormatException or TimeZoneNotFoundException or
                                    InvalidTimeZoneException or InvalidOperationException)
        {
            await connection.ExecuteAsync(new CommandDefinition("""
                UPDATE Kj_JobSpecs SET IsDisabled=TRUE, ScheduleError=LEFT(@Error,1000),
                    ScheduleErrorAt=@Now, UpdatedAt=@Now WHERE Id=@Id;
                """, new { spec.Id, Error = ex.Message, Now = now }, transaction,
                cancellationToken: cancellationToken));
            await transaction.CommitAsync(cancellationToken);
            return new OneResult(true, 0, false, false, true);
        }

        // Registration seeds definitions but does not surprise users by immediately firing them.
        if (spec.NextRunTime is null)
        {
            await AdvanceAsync(connection, transaction, spec.Id, next, now, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new OneResult(true, 0, false, false, false);
        }

        var active = await connection.ExecuteScalarAsync<bool>(new CommandDefinition("""
            SELECT EXISTS(SELECT 1 FROM Kj_JobRuns
                WHERE SpecId=@Id AND Status IN (@Pending,@Assigned,@Running));
            """, new
        {
            spec.Id,
            Pending=(int)JobStatus.Pending,
            Assigned=(int)JobStatus.Assigned,
            Running=(int)JobStatus.Running
        }, transaction, cancellationToken:cancellationToken));

        if (active && spec.ConcurrencyPolicy == ConcurrencyPolicy.Forbid)
        {
            await AdvanceAsync(connection, transaction, spec.Id, next, now, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new OneResult(true, 0, true, false, false);
        }
        if (active && spec.ConcurrencyPolicy == ConcurrencyPolicy.Replace)
        {
            await connection.ExecuteAsync(new CommandDefinition("""
                UPDATE Kj_JobRuns SET CancelRequestedAt=COALESCE(CancelRequestedAt,@Now),
                    Status=CASE WHEN Status IN (@Pending,@Assigned) THEN @Canceled ELSE Status END,
                    EndTime=CASE WHEN Status IN (@Pending,@Assigned) THEN @Now ELSE EndTime END,
                    ResultMsg=CASE WHEN Status IN (@Pending,@Assigned) THEN 'Canceled by ConcurrencyPolicy.Replace' ELSE ResultMsg END
                WHERE SpecId=@Id AND Status IN (@Pending,@Assigned,@Running);
                """, new
            {
                spec.Id, Now=now, Pending=(int)JobStatus.Pending, Assigned=(int)JobStatus.Assigned,
                Running=(int)JobStatus.Running, Canceled=(int)JobStatus.Canceled
            }, transaction, cancellationToken:cancellationToken));
        }

        var scheduledAt = spec.NextRunTime.Value;
        var batchId = Hash128(string.Concat(spec.Id, "|", scheduledAt.UtcDateTime.Ticks.ToString(CultureInfo.InvariantCulture)));
        int count;
        if (spec.ExecuteModel == ExecuteModel.Broadcast)
        {
            var targetCount = await CountBroadcastTargetsAsync(connection, transaction, spec, now, cancellationToken);
            if (targetCount > MaxShards)
            {
                await connection.ExecuteAsync(new CommandDefinition("""
                    UPDATE Kj_JobSpecs SET IsDisabled=TRUE,
                        ScheduleError=@Error, ScheduleErrorAt=@Now, UpdatedAt=@Now WHERE Id=@Id;
                    """, new
                {
                    spec.Id,
                    Error = $"Broadcast target count exceeds {MaxShards}; narrow NodeSelector.",
                    Now = now
                }, transaction, cancellationToken: cancellationToken));
                await transaction.CommitAsync(cancellationToken);
                return new OneResult(true, 0, false, false, true);
            }
            count = targetCount == 0 ? 0 : await InsertBroadcastAsync(
                connection, transaction, spec, batchId, scheduledAt, now, cancellationToken);
        }
        else
        {
            count = await InsertSeriesAsync(connection, transaction, spec, batchId, scheduledAt, now, cancellationToken);
        }

        await AdvanceAsync(connection, transaction, spec.Id, next, now, cancellationToken);
        if (count > 0) await PostgreSqlQueueSignal.NotifyAsync(connection, transaction, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new OneResult(true, count, false, spec.ExecuteModel == ExecuteModel.Broadcast && count == 0, false);
    }

    private static async Task<int> InsertSeriesAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        DueSpec spec, string batchId, DateTimeOffset scheduledAt, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var count = spec.ExecuteModel switch
        {
            ExecuteModel.Standalone => 1,
            ExecuteModel.Sharding => Math.Clamp(spec.TotalShards, 1, MaxShards),
            _ => throw new InvalidOperationException($"Unsupported execution model {(int)spec.ExecuteModel}.")
        };
        const string sql = """
            INSERT INTO Kj_JobRuns
                (Id,SpecId,BatchId,ShardIndex,BatchSize,Status,TargetNodeId,CreatedAt,ResultMsg,RowVersion,
                 Attempt,LeaseToken,WorkerSessionEpoch,AvailableAt,ScheduledAt,QueueName,Priority,PayloadJson,
                 IdempotencyKey,JobType,TimeoutSeconds,MaxRetries,NodeSelector,RequiredHandlerVersion,
                 PayloadSchemaVersion,PinnedWorkerId,PinnedSessionEpoch)
            SELECT md5(@BatchId || ':' || g::text), @SpecId, @BatchId, g, @BatchSize, @Pending, NULL,
                   @Now, '', '', 0,0,0,@Now,@ScheduledAt,@QueueName,@Priority,'{}'::jsonb,
                   @BatchId,@JobType,@TimeoutSeconds,@MaxRetries,@NodeSelector::jsonb,
                   @RequiredHandlerVersion,@PayloadSchemaVersion,NULL,NULL
            FROM generate_series(0,@BatchSize-1) AS g
            ON CONFLICT DO NOTHING;
            """;
        return await connection.ExecuteAsync(new CommandDefinition(sql, new
        {
            BatchId=batchId, SpecId=spec.Id, BatchSize=count, Pending=(int)JobStatus.Pending,
            Now=now, ScheduledAt=scheduledAt, spec.QueueName, spec.Priority, spec.JobType,
            spec.TimeoutSeconds, spec.MaxRetries, NodeSelector=spec.NodeSelectorJson,
            spec.RequiredHandlerVersion, spec.PayloadSchemaVersion
        }, transaction, cancellationToken:cancellationToken));
    }

    private static Task<int> CountBroadcastTargetsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DueSpec spec,
        DateTimeOffset now,
        CancellationToken cancellationToken) =>
        connection.ExecuteScalarAsync<int>(new CommandDefinition("""
            SELECT COUNT(*)::int FROM (
                SELECT 1 FROM Kj_WorkerNodes w
                JOIN Kj_WorkerCapabilities c ON c.WorkerId=w.Id AND c.SessionEpoch=w.SessionEpoch
                    AND c.JobType=@JobType
                WHERE w.IsOffline=FALSE AND w.Draining=FALSE AND w.LastHeartbeat>=@HeartbeatCutoff
                  AND COALESCE(w.Labels,'{}'::jsonb) @> @NodeSelector::jsonb
                  AND (@RequiredHandlerVersion='' OR c.HandlerVersion=@RequiredHandlerVersion)
                  AND c.PayloadSchemaVersion>=@PayloadSchemaVersion
                LIMIT @Limit
            ) AS bounded;
            """, new
        {
            spec.JobType,
            HeartbeatCutoff = now - TimeSpan.FromSeconds(30),
            NodeSelector = spec.NodeSelectorJson,
            spec.RequiredHandlerVersion,
            spec.PayloadSchemaVersion,
            Limit = MaxShards + 1
        }, transaction, cancellationToken: cancellationToken));

    private static async Task<int> InsertBroadcastAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        DueSpec spec, string batchId, DateTimeOffset scheduledAt, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        const string sql = """
            WITH targets AS (
                SELECT w.Id AS WorkerId, w.SessionEpoch,
                       row_number() OVER(ORDER BY w.Id,w.SessionEpoch)-1 AS ShardIndex,
                       count(*) OVER() AS BatchSize
                FROM Kj_WorkerNodes w
                JOIN Kj_WorkerCapabilities c ON c.WorkerId=w.Id AND c.SessionEpoch=w.SessionEpoch
                    AND c.JobType=@JobType
                WHERE w.IsOffline=FALSE AND w.Draining=FALSE AND w.LastHeartbeat>=@HeartbeatCutoff
                  AND COALESCE(w.Labels,'{}'::jsonb) @> @NodeSelector::jsonb
                  AND (@RequiredHandlerVersion='' OR c.HandlerVersion=@RequiredHandlerVersion)
                  AND c.PayloadSchemaVersion>=@PayloadSchemaVersion
                ORDER BY w.Id,w.SessionEpoch LIMIT @Limit
            )
            INSERT INTO Kj_JobRuns
                (Id,SpecId,BatchId,ShardIndex,BatchSize,Status,TargetNodeId,CreatedAt,ResultMsg,RowVersion,
                 Attempt,LeaseToken,WorkerSessionEpoch,AvailableAt,ScheduledAt,QueueName,Priority,PayloadJson,
                 IdempotencyKey,JobType,TimeoutSeconds,MaxRetries,NodeSelector,RequiredHandlerVersion,
                 PayloadSchemaVersion,PinnedWorkerId,PinnedSessionEpoch)
            SELECT md5(@BatchId || ':' || WorkerId || ':' || SessionEpoch::text), @SpecId,@BatchId,
                   ShardIndex,BatchSize,@Pending,NULL,@Now,'','',0,0,0,@Now,@ScheduledAt,@QueueName,@Priority,
                   '{}'::jsonb,@BatchId,@JobType,@TimeoutSeconds,@MaxRetries,@NodeSelector::jsonb,
                   @RequiredHandlerVersion,@PayloadSchemaVersion,WorkerId,SessionEpoch
            FROM targets ON CONFLICT DO NOTHING;
            """;
        var inserted = await connection.ExecuteAsync(new CommandDefinition(sql, new
        {
            BatchId=batchId, SpecId=spec.Id, Pending=(int)JobStatus.Pending, Now=now,
            ScheduledAt=scheduledAt, spec.QueueName, spec.Priority, spec.JobType,
            spec.TimeoutSeconds, spec.MaxRetries, NodeSelector=spec.NodeSelectorJson,
            spec.RequiredHandlerVersion, spec.PayloadSchemaVersion,
            HeartbeatCutoff=now-TimeSpan.FromSeconds(30), Limit=MaxShards
        }, transaction, cancellationToken:cancellationToken));
        if (inserted > MaxShards)
            throw new InvalidOperationException($"Broadcast target count exceeds {MaxShards}.");
        return inserted;
    }

    private static Task AdvanceAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string id, DateTimeOffset next, DateTimeOffset now, CancellationToken cancellationToken) =>
        connection.ExecuteAsync(new CommandDefinition("""
            UPDATE Kj_JobSpecs SET NextRunTime=@Next, UpdatedAt=@Now,
                ScheduleError='', ScheduleErrorAt=NULL WHERE Id=@Id;
            """, new { Id=id, Next=next, Now=now }, transaction,
            cancellationToken:cancellationToken));

    private static string Hash128(string input) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(input)).AsSpan(0,16)).ToLowerInvariant();

    private readonly record struct OneResult(bool Found, int Inserted, bool Skipped, bool EmptyBroadcast, bool Invalid);
    private sealed class DueSpec
    {
        public string Id { get; init; }=string.Empty;
        public string JobType { get; init; }=string.Empty;
        public string CronExpression { get; init; }=string.Empty;
        public string NodeSelectorJson { get; init; }="{}";
        public ExecuteModel ExecuteModel { get; init; }
        public int TotalShards { get; init; }
        public DateTimeOffset? NextRunTime { get; init; }
        public int TimeoutSeconds { get; init; }
        public int MaxRetries { get; init; }
        public ConcurrencyPolicy ConcurrencyPolicy { get; init; }
        public string QueueName { get; init; }="default";
        public int Priority { get; init; }
        public string RequiredHandlerVersion { get; init; }=string.Empty;
        public int PayloadSchemaVersion { get; init; }=1;
        public string ScheduleTimeZone { get; init; }="UTC";
    }
}
