using System.Data;
using System.Text.Json;
using Dapper;
using KubeJob.Core.Domain;
using KubeJob.Core.Dtos;
using KubeJob.Core.Enums;
using KubeJob.Server.Data;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Data;

/// <summary>PostgreSQL-backed, pull-based execution leases with session and attempt fencing.</summary>
public sealed class PostgreSqlRuntimeRepository : IKubeJobRuntimeRepository
{
    private const int MaxClaimBatch = 256;
    private const int MaxReaperBatch = 4096;
    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlRuntimeRepository(NpgsqlDataSource dataSource) =>
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));

    public async Task<long> RegisterWorkerSessionAsync(RegisterWorkerSessionRequest request,
        string labelsJson, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        var epoch = await connection.ExecuteScalarAsync<long>(new CommandDefinition(
            "SELECT nextval('Kj_WorkerSessionEpochSeq')", transaction: transaction,
            cancellationToken: cancellationToken));
        const string workerSql = """
            INSERT INTO Kj_WorkerNodes
                (Id, IpAddress, Labels, LastHeartbeat, CurrentLoad, MaxCapacity, IsOffline,
                 SessionId, SessionEpoch, RuntimeVersion, Draining)
            VALUES
                (@WorkerId, @IpAddress, @Labels::jsonb, clock_timestamp(), 0, @MaxCapacity, FALSE,
                 @SessionId, @Epoch, @RuntimeVersion, FALSE)
            ON CONFLICT (Id) DO UPDATE SET
                IpAddress = EXCLUDED.IpAddress,
                Labels = EXCLUDED.Labels,
                LastHeartbeat = EXCLUDED.LastHeartbeat,
                CurrentLoad = 0,
                MaxCapacity = EXCLUDED.MaxCapacity,
                IsOffline = FALSE,
                SessionId = EXCLUDED.SessionId,
                SessionEpoch = EXCLUDED.SessionEpoch,
                RuntimeVersion = EXCLUDED.RuntimeVersion,
                Draining = FALSE;
            """;
        await connection.ExecuteAsync(new CommandDefinition(workerSql, new
        {
            request.WorkerId,
            request.SessionId,
            request.IpAddress,
            Labels = labelsJson,
            request.MaxCapacity,
            request.RuntimeVersion,
            Epoch = epoch
        }, transaction, cancellationToken: cancellationToken));

        await connection.ExecuteAsync(new CommandDefinition(
            "DELETE FROM Kj_WorkerCapabilities WHERE WorkerId = @WorkerId",
            new { request.WorkerId }, transaction, cancellationToken: cancellationToken));

        var count = request.Capabilities.Count;
        var types = new string[count];
        var versions = new string[count];
        var schemas = new int[count];
        for (var i = 0; i < count; i++)
        {
            types[i] = request.Capabilities[i].JobType;
            versions[i] = request.Capabilities[i].HandlerVersion ?? string.Empty;
            schemas[i] = request.Capabilities[i].PayloadSchemaVersion;
        }
        const string capsSql = """
            INSERT INTO Kj_WorkerCapabilities
                (WorkerId, SessionEpoch, JobType, HandlerVersion, PayloadSchemaVersion)
            SELECT @WorkerId, @Epoch, x.JobType, x.HandlerVersion, x.PayloadSchemaVersion
            FROM unnest(@Types::text[], @Versions::text[], @Schemas::integer[])
                AS x(JobType, HandlerVersion, PayloadSchemaVersion);
            """;
        await connection.ExecuteAsync(new CommandDefinition(capsSql, new
        {
            request.WorkerId,
            Epoch = epoch,
            Types = types,
            Versions = versions,
            Schemas = schemas
        }, transaction, cancellationToken: cancellationToken));

        // Defaults are seed data only. Existing dashboard/database configuration is authoritative.
        var definitions = request.Definitions;
        var names = new string[definitions.Count];
        var crons = new string[definitions.Count];
        var executeModels = new int[definitions.Count];
        var totalShards = new int[definitions.Count];
        var timeouts = new int[definitions.Count];
        var retries = new int[definitions.Count];
        var selectors = new string[definitions.Count];
        for (var i = 0; i < definitions.Count; i++)
        {
            var definition = definitions[i];
            names[i] = definition.Name;
            crons[i] = definition.Cron ?? string.Empty;
            executeModels[i] = (int)definition.ExecuteModel;
            totalShards[i] = definition.TotalShards;
            timeouts[i] = definition.TimeoutSeconds;
            retries[i] = definition.MaxRetries;
            selectors[i] = JsonSerializer.Serialize(definition.NodeSelectors);
        }
        const string definitionsSql = """
            WITH input AS (
                SELECT * FROM unnest(
                    @Names::text[], @Crons::text[], @Models::integer[], @Shards::integer[],
                    @Timeouts::integer[], @Retries::integer[], @Selectors::text[])
                    AS x(Name, Cron, ExecuteModel, TotalShards, TimeoutSeconds, MaxRetries, NodeSelector)
            )
            INSERT INTO Kj_JobSpecs
                (Id, Name, JobType, CronExpression, NodeSelector, ExecuteModel, TotalShards,
                 NextRunTime, IsDisabled, TimeoutSeconds, MaxRetries, ConcurrencyPolicy,
                 SuccessfulJobsHistoryLimit, FailedJobsHistoryLimit,
                 QueueName, Priority, RequiredHandlerVersion, PayloadSchemaVersion,
                 ScheduleTimeZone, ScheduleError)
            SELECT
                md5(Name), Name, Name, NULLIF(Cron, ''), NodeSelector::jsonb, ExecuteModel,
                TotalShards, NULL, FALSE, TimeoutSeconds, MaxRetries, 0, 3, 1,
                'default', 0, '', 1, 'UTC', ''
            FROM input
            ON CONFLICT (Name) DO NOTHING;
            """;
        await connection.ExecuteAsync(new CommandDefinition(definitionsSql, new
        {
            Names = names,
            Crons = crons,
            Models = executeModels,
            Shards = totalShards,
            Timeouts = timeouts,
            Retries = retries,
            Selectors = selectors
        }, transaction, cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return epoch;
    }

    public async Task<IReadOnlyList<JobLease>> ClaimRunsAsync(string workerId, string sessionId,
        long sessionEpoch, IReadOnlyList<string> queueNames, int limit, TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        const string lockWorkerSql = """
            SELECT MaxCapacity, Draining
            FROM Kj_WorkerNodes
            WHERE Id = @WorkerId AND SessionId = @SessionId AND SessionEpoch = @SessionEpoch
            FOR UPDATE;
            """;
        var worker = await connection.QuerySingleOrDefaultAsync<WorkerSessionRow>(new CommandDefinition(
            lockWorkerSql, new { WorkerId = workerId, SessionId = sessionId, SessionEpoch = sessionEpoch },
            transaction, cancellationToken: cancellationToken));
        if (worker is null || worker.Draining)
        {
            await transaction.CommitAsync(cancellationToken);
            return Array.Empty<JobLease>();
        }

        const string activeSql = """
            SELECT COUNT(*)::int
            FROM Kj_JobRuns
            WHERE TargetNodeId = @WorkerId
              AND WorkerSessionEpoch = @SessionEpoch
              AND Status IN (@Assigned, @Running);
            """;
        var active = await connection.ExecuteScalarAsync<int>(new CommandDefinition(activeSql, new
        {
            WorkerId = workerId,
            SessionEpoch = sessionEpoch,
            Assigned = (int)JobStatus.Assigned,
            Running = (int)JobStatus.Running
        }, transaction, cancellationToken: cancellationToken));
        var bounded = Math.Min(Math.Clamp(limit, 1, MaxClaimBatch), Math.Max(0, worker.MaxCapacity - active));
        if (bounded == 0)
        {
            await transaction.CommitAsync(cancellationToken);
            return Array.Empty<JobLease>();
        }

        var queues = queueNames.Count == 0 ? Array.Empty<string>() : queueNames.ToArray();
        const string claimSql = """
            WITH candidates AS (
                SELECT r.Id
                FROM Kj_JobRuns r
                JOIN Kj_WorkerNodes w
                  ON w.Id = @WorkerId
                 AND w.SessionId = @SessionId
                 AND w.SessionEpoch = @SessionEpoch
                 AND w.Draining = FALSE
                 AND w.IsOffline = FALSE
                JOIN Kj_WorkerCapabilities c
                  ON c.WorkerId = w.Id
                 AND c.SessionEpoch = w.SessionEpoch
                 AND c.JobType = r.JobType
                WHERE r.Status = @Pending
                  AND r.AvailableAt <= clock_timestamp()
                  AND r.CancelRequestedAt IS NULL
                  AND (cardinality(@QueueNames::text[]) = 0 OR r.QueueName = ANY(@QueueNames))
                  AND COALESCE(w.Labels, '{}'::jsonb) @> COALESCE(r.NodeSelector, '{}'::jsonb)
                  AND (r.RequiredHandlerVersion = '' OR c.HandlerVersion = r.RequiredHandlerVersion)
                  AND c.PayloadSchemaVersion >= r.PayloadSchemaVersion
                  AND (r.PinnedWorkerId IS NULL OR
                       (r.PinnedWorkerId = w.Id AND r.PinnedSessionEpoch = w.SessionEpoch))
                ORDER BY r.Priority DESC, r.AvailableAt, r.CreatedAt, r.Id
                FOR UPDATE OF r SKIP LOCKED
                LIMIT @Limit
            ), claimed AS (
                UPDATE Kj_JobRuns r
                SET Status = @Running,
                    TargetNodeId = @WorkerId,
                    WorkerSessionEpoch = @SessionEpoch,
                    Attempt = r.Attempt + 1,
                    LeaseToken = nextval('Kj_LeaseTokenSeq'),
                    StartTime = COALESCE(r.StartTime, clock_timestamp()),
                    LeaseExpiresAt = clock_timestamp() + make_interval(secs => @LeaseSeconds),
                    LastLeaseRenewalAt = clock_timestamp()
                FROM candidates c
                WHERE r.Id = c.Id
                RETURNING r.Id AS RunId, r.SpecId, r.BatchId, r.JobType,
                          r.PayloadSchemaVersion, r.Attempt, r.LeaseToken, r.LeaseExpiresAt,
                          r.TimeoutSeconds, r.ShardIndex, r.BatchSize AS TotalShards, r.ScheduledAt
            ), attempt_insert AS (
                INSERT INTO Kj_JobRunAttempts
                    (RunId, Attempt, LeaseToken, WorkerId, WorkerSessionEpoch, Status, AssignedAt, StartedAt)
                SELECT RunId, Attempt, LeaseToken, @WorkerId, @SessionEpoch, @Running,
                       clock_timestamp(), clock_timestamp()
                FROM claimed
                RETURNING RunId
            )
            SELECT c.RunId, c.SpecId, c.BatchId, c.JobType,
                   COALESCE(p.PayloadJson, '{}'::jsonb)::text AS PayloadJson,
                   c.PayloadSchemaVersion, c.Attempt, c.LeaseToken, c.LeaseExpiresAt,
                   c.TimeoutSeconds, c.ShardIndex, c.TotalShards, c.ScheduledAt
            FROM claimed c
            LEFT JOIN Kj_JobPayloads p ON p.BatchId = c.BatchId
            ORDER BY c.RunId;
            """;
        var leases = (await connection.QueryAsync<JobLease>(new CommandDefinition(claimSql, new
        {
            WorkerId = workerId,
            SessionId = sessionId,
            SessionEpoch = sessionEpoch,
            QueueNames = queues,
            Limit = bounded,
            Pending = (int)JobStatus.Pending,
            Running = (int)JobStatus.Running,
            LeaseSeconds = Math.Max(1d, leaseDuration.TotalSeconds)
        }, transaction, cancellationToken: cancellationToken))).AsList();

        await connection.ExecuteAsync(new CommandDefinition(
            "UPDATE Kj_WorkerNodes SET LastHeartbeat=clock_timestamp(), CurrentLoad=@Load WHERE Id=@WorkerId AND SessionEpoch=@Epoch",
            new { Load = active + leases.Count, WorkerId = workerId, Epoch = sessionEpoch },
            transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
        return leases;
    }

    public async Task<RenewLeasesResponse> RenewLeasesAsync(RenewLeasesRequest request,
        TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        var valid = await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE Kj_WorkerNodes
            SET LastHeartbeat=clock_timestamp(), CurrentLoad=@CurrentLoad, Draining=@Draining, IsOffline=FALSE
            WHERE Id=@WorkerId AND SessionId=@SessionId AND SessionEpoch=@SessionEpoch;
            """, new
        {
            request.WorkerId, request.SessionId, request.SessionEpoch,
            request.CurrentLoad, request.Draining
        }, transaction, cancellationToken: cancellationToken));

        if (valid == 0)
        {
            await transaction.CommitAsync(cancellationToken);
            return new RenewLeasesResponse
            {
                RejectedRunIds = request.Leases.Select(static x => x.RunId).ToArray()
            };
        }

        var ids = new string[request.Leases.Count];
        var tokens = new long[request.Leases.Count];
        for (var i = 0; i < request.Leases.Count; i++)
        {
            ids[i] = request.Leases[i].RunId;
            tokens[i] = request.Leases[i].LeaseToken;
        }
        const string sql = """
            WITH input AS (
                SELECT * FROM unnest(@Ids::text[], @Tokens::bigint[]) AS x(RunId, LeaseToken)
            )
            UPDATE Kj_JobRuns r
            SET LeaseExpiresAt=clock_timestamp() + make_interval(secs => @LeaseSeconds), LastLeaseRenewalAt=clock_timestamp()
            FROM input i
            WHERE r.Id=i.RunId AND r.LeaseToken=i.LeaseToken
              AND r.TargetNodeId=@WorkerId AND r.WorkerSessionEpoch=@SessionEpoch
              AND r.Status IN (@Assigned,@Running) AND r.LeaseExpiresAt>=clock_timestamp()
            RETURNING r.Id AS RunId, r.CancelRequestedAt IS NOT NULL AS CancelRequested;
            """;
        var renewed = (await connection.QueryAsync<RenewedLeaseRow>(new CommandDefinition(sql, new
        {
            Ids = ids, Tokens = tokens, request.WorkerId, request.SessionEpoch,
            Assigned = (int)JobStatus.Assigned, Running = (int)JobStatus.Running,
            LeaseSeconds = Math.Max(1d, leaseDuration.TotalSeconds)
        }, transaction, cancellationToken: cancellationToken))).AsList();
        await transaction.CommitAsync(cancellationToken);

        var accepted = renewed.Select(static x => x.RunId).ToHashSet(StringComparer.Ordinal);
        return new RenewLeasesResponse
        {
            RejectedRunIds = ids.Where(x => !accepted.Contains(x)).ToArray(),
            CancelRunIds = renewed.Where(static x => x.CancelRequested).Select(static x => x.RunId).ToArray()
        };
    }

    public async Task<bool> TryCompleteRunAsync(CompleteRunRequest request,
        CancellationToken cancellationToken)
    {
        if (request.Status is not (JobStatus.Succeeded or JobStatus.Failed or JobStatus.Canceled)) return false;
        const string sql = """
            WITH valid_worker AS (
                SELECT 1 FROM Kj_WorkerNodes
                WHERE Id=@WorkerId AND SessionId=@SessionId AND SessionEpoch=@SessionEpoch
            ), locked AS (
                SELECT r.Id, r.Attempt, r.CancelRequestedAt, r.MaxRetries
                FROM Kj_JobRuns r
                WHERE r.Id=@RunId AND r.Status=@Running AND r.TargetNodeId=@WorkerId
                  AND r.WorkerSessionEpoch=@SessionEpoch AND r.LeaseToken=@LeaseToken
                  AND r.LeaseExpiresAt>=clock_timestamp() AND EXISTS(SELECT 1 FROM valid_worker)
                FOR UPDATE OF r
            ), updated AS (
                UPDATE Kj_JobRuns r SET
                    Status=CASE
                        WHEN l.CancelRequestedAt IS NOT NULL THEN @Canceled
                        WHEN @ReportedStatus=@Failed AND l.Attempt<=l.MaxRetries THEN @Pending
                        ELSE @ReportedStatus END,
                    AvailableAt=CASE
                        WHEN @ReportedStatus=@Failed AND l.CancelRequestedAt IS NULL AND l.Attempt<=l.MaxRetries
                        THEN clock_timestamp() + LEAST(300.0, POWER(2.0,GREATEST(0,l.Attempt-1))) * INTERVAL '1 second'
                        ELSE r.AvailableAt END,
                    TargetNodeId=CASE WHEN @ReportedStatus=@Failed AND l.CancelRequestedAt IS NULL AND l.Attempt<=l.MaxRetries THEN NULL ELSE r.TargetNodeId END,
                    WorkerSessionEpoch=CASE WHEN @ReportedStatus=@Failed AND l.CancelRequestedAt IS NULL AND l.Attempt<=l.MaxRetries THEN 0 ELSE r.WorkerSessionEpoch END,
                    LeaseToken=CASE WHEN @ReportedStatus=@Failed AND l.CancelRequestedAt IS NULL AND l.Attempt<=l.MaxRetries THEN 0 ELSE r.LeaseToken END,
                    LeaseExpiresAt=NULL, LastLeaseRenewalAt=NULL,
                    EndTime=CASE WHEN @ReportedStatus=@Failed AND l.CancelRequestedAt IS NULL AND l.Attempt<=l.MaxRetries THEN NULL ELSE clock_timestamp() END,
                    ResultMsg=LEFT(@ResultSummary,2000)
                FROM locked l WHERE r.Id=l.Id
                RETURNING r.Id, r.Attempt, r.Status AS NewStatus
            ), attempt_update AS (
                UPDATE Kj_JobRunAttempts a SET
                    Status=CASE WHEN EXISTS(SELECT 1 FROM locked l WHERE l.CancelRequestedAt IS NOT NULL) THEN @Canceled ELSE @ReportedStatus END,
                    EndedAt=clock_timestamp(), ErrorSummary=LEFT(@ResultSummary,2000)
                FROM updated u WHERE a.RunId=u.Id AND a.Attempt=u.Attempt RETURNING 1
            )
            SELECT EXISTS(SELECT 1 FROM updated) AS Completed,
                   EXISTS(SELECT 1 FROM updated WHERE NewStatus=@Pending) AS Requeued;
            """;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var result = await connection.QuerySingleAsync<CompletionResultRow>(new CommandDefinition(sql, new
        {
            request.WorkerId, request.SessionId, request.SessionEpoch, request.RunId, request.LeaseToken,
            ReportedStatus = (int)request.Status,
            Pending = (int)JobStatus.Pending, Running = (int)JobStatus.Running,
            Failed = (int)JobStatus.Failed, Canceled = (int)JobStatus.Canceled,
            ResultSummary = request.ResultSummary ?? string.Empty
        }, cancellationToken: cancellationToken));
        if (result.Requeued) await PostgreSqlQueueSignal.NotifyAsync(connection, null, cancellationToken);
        return result.Completed;
    }

    public async Task<int> RequeueExpiredLeasesAsync(int limit, CancellationToken cancellationToken)
    {
        const string sql = """
            WITH candidates AS (
                SELECT r.Id, r.Attempt, r.CancelRequestedAt, r.MaxRetries
                FROM Kj_JobRuns r
                WHERE r.Status IN (@Assigned,@Running) AND r.LeaseExpiresAt<clock_timestamp()
                ORDER BY r.LeaseExpiresAt, r.Id FOR UPDATE OF r SKIP LOCKED LIMIT @Limit
            ), updated AS (
                UPDATE Kj_JobRuns r SET
                    Status=CASE WHEN c.CancelRequestedAt IS NOT NULL THEN @Canceled
                                WHEN c.Attempt<=c.MaxRetries THEN @Pending ELSE @Failed END,
                    AvailableAt=CASE WHEN c.CancelRequestedAt IS NULL AND c.Attempt<=c.MaxRetries
                        THEN clock_timestamp() + LEAST(300.0,POWER(2.0,GREATEST(0,c.Attempt-1))) * INTERVAL '1 second'
                        ELSE r.AvailableAt END,
                    TargetNodeId=NULL, WorkerSessionEpoch=0, LeaseToken=0,
                    LeaseExpiresAt=NULL, LastLeaseRenewalAt=NULL,
                    EndTime=CASE WHEN c.CancelRequestedAt IS NOT NULL OR c.Attempt>c.MaxRetries THEN clock_timestamp() ELSE NULL END,
                    ResultMsg=CASE WHEN c.CancelRequestedAt IS NOT NULL THEN 'Canceled after lease expiry'
                                   WHEN c.Attempt>c.MaxRetries THEN 'Lease expired; retry limit exhausted'
                                   ELSE 'Lease expired; queued for retry' END
                FROM candidates c WHERE r.Id=c.Id
                RETURNING r.Id, r.Attempt,
                    CASE WHEN c.CancelRequestedAt IS NOT NULL THEN @Canceled ELSE @Failed END AS AttemptStatus
            ), attempt_update AS (
                UPDATE Kj_JobRunAttempts a SET Status=u.AttemptStatus, EndedAt=clock_timestamp(),
                    ErrorSummary='Worker lease expired'
                FROM updated u WHERE a.RunId=u.Id AND a.Attempt=u.Attempt RETURNING 1
            ) SELECT COUNT(*)::int FROM updated;
            """;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var count = await connection.ExecuteScalarAsync<int>(new CommandDefinition(sql, new
        {
            Assigned=(int)JobStatus.Assigned, Running=(int)JobStatus.Running,
            Pending=(int)JobStatus.Pending, Failed=(int)JobStatus.Failed, Canceled=(int)JobStatus.Canceled,
            Limit=Math.Clamp(limit,1,MaxReaperBatch)
        }, cancellationToken:cancellationToken));
        if (count>0) await PostgreSqlQueueSignal.NotifyAsync(connection,null,cancellationToken);
        return count;
    }

    public async Task<int> FinalizeOrphanedPinnedRunsAsync(TimeSpan heartbeatTimeout,
        int limit, CancellationToken cancellationToken)
    {
        const string sql = """
            WITH candidates AS (
                SELECT r.Id FROM Kj_JobRuns r
                LEFT JOIN Kj_WorkerNodes w ON w.Id=r.PinnedWorkerId AND w.SessionEpoch=r.PinnedSessionEpoch
                WHERE r.Status=@Pending AND r.PinnedWorkerId IS NOT NULL
                  AND (w.Id IS NULL OR w.IsOffline=TRUE OR w.Draining=TRUE OR w.LastHeartbeat < clock_timestamp() - make_interval(secs => @HeartbeatTimeoutSeconds))
                ORDER BY r.CreatedAt, r.Id FOR UPDATE OF r SKIP LOCKED LIMIT @Limit
            ) UPDATE Kj_JobRuns r SET Status=@Failed, EndTime=clock_timestamp(),
                ResultMsg='Broadcast target session became unavailable before claim'
              FROM candidates c WHERE r.Id=c.Id;
            """;
        await using var connection=await _dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteAsync(new CommandDefinition(sql,new
        {
            Pending=(int)JobStatus.Pending, Failed=(int)JobStatus.Failed,
            HeartbeatTimeoutSeconds=Math.Max(1d,heartbeatTimeout.TotalSeconds), Limit=Math.Clamp(limit,1,MaxReaperBatch)
        },cancellationToken:cancellationToken));
    }

    public async Task<int> CleanupOrphanedBatchMetadataAsync(TimeSpan idempotencyRetention,
        int limit, CancellationToken cancellationToken)
    {
        await using var connection=await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction=await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted,cancellationToken);
        var bounded=Math.Clamp(limit,1,MaxReaperBatch);
        var attempts=await connection.ExecuteAsync(new CommandDefinition("""
            WITH c AS (SELECT a.RunId,a.Attempt FROM Kj_JobRunAttempts a
                WHERE NOT EXISTS(SELECT 1 FROM Kj_JobRuns r WHERE r.Id=a.RunId)
                ORDER BY a.AssignedAt FOR UPDATE OF a SKIP LOCKED LIMIT @Limit)
            DELETE FROM Kj_JobRunAttempts a USING c WHERE a.RunId=c.RunId AND a.Attempt=c.Attempt;
            """,new{Limit=bounded},transaction,cancellationToken:cancellationToken));
        var payloads=await connection.ExecuteAsync(new CommandDefinition("""
            WITH c AS (SELECT p.BatchId FROM Kj_JobPayloads p
                WHERE NOT EXISTS(SELECT 1 FROM Kj_JobRuns r WHERE r.BatchId=p.BatchId)
                ORDER BY p.CreatedAt FOR UPDATE OF p SKIP LOCKED LIMIT @Limit)
            DELETE FROM Kj_JobPayloads p USING c WHERE p.BatchId=c.BatchId;
            """,new{Limit=bounded},transaction,cancellationToken:cancellationToken));
        var submissions=await connection.ExecuteAsync(new CommandDefinition("""
            WITH c AS (SELECT s.SpecId,s.IdempotencyKey FROM Kj_JobSubmissions s
                WHERE s.CreatedAt < clock_timestamp() - make_interval(secs => @RetentionSeconds) AND NOT EXISTS(SELECT 1 FROM Kj_JobRuns r WHERE r.BatchId=s.BatchId)
                ORDER BY s.CreatedAt FOR UPDATE OF s SKIP LOCKED LIMIT @Limit)
            DELETE FROM Kj_JobSubmissions s USING c
            WHERE s.SpecId=c.SpecId AND s.IdempotencyKey=c.IdempotencyKey;
            """,new{RetentionSeconds=Math.Max(1d,idempotencyRetention.TotalSeconds),Limit=bounded},transaction,cancellationToken:cancellationToken));
        await transaction.CommitAsync(cancellationToken);
        return attempts+payloads+submissions;
    }

    private sealed class WorkerSessionRow { public int MaxCapacity { get; init; } public bool Draining { get; init; } }
    private sealed class RenewedLeaseRow { public string RunId { get; init; }=string.Empty; public bool CancelRequested { get; init; } }
    private sealed class CompletionResultRow { public bool Completed { get; init; } public bool Requeued { get; init; } }
}
