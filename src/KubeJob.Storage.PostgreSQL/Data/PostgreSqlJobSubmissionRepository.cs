using System.Data;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Dapper;
using KubeJob.Core.Domain;
using KubeJob.Core.Enums;
using KubeJob.Server.Data;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Data;

public sealed class PostgreSqlJobSubmissionRepository : IKubeJobSubmissionRepository
{
    private const int MaxShards = 4096;
    private readonly NpgsqlDataSource _dataSource;
    public PostgreSqlJobSubmissionRepository(NpgsqlDataSource dataSource) => _dataSource = dataSource;

    public async Task<JobSubmissionResult> SubmitAsync(JobSubmissionCommand command,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        const string specSql = """
            SELECT Id,Name,JobType,COALESCE(NodeSelector,'{}'::jsonb)::text AS NodeSelectorJson,
                   ExecuteModel,GREATEST(1,TotalShards) AS TotalShards,TimeoutSeconds,MaxRetries,
                   QueueName,Priority,RequiredHandlerVersion,PayloadSchemaVersion
            FROM Kj_JobSpecs WHERE Name=@JobName FOR SHARE;
            """;
        var spec = await connection.QuerySingleOrDefaultAsync<SubmissionSpec>(new CommandDefinition(
            specSql,new{command.JobName},transaction,cancellationToken:cancellationToken))
            ?? throw new KeyNotFoundException($"KubeJob '{command.JobName}' is not registered.");
        if (command.PayloadSchemaVersion < spec.PayloadSchemaVersion)
            throw new InvalidOperationException($"Payload schema v{command.PayloadSchemaVersion} is older than required v{spec.PayloadSchemaVersion}.");

        var batchId = string.IsNullOrEmpty(command.IdempotencyKey)
            ? Guid.NewGuid().ToString("N")
            : Hash128(string.Concat(spec.Id,"|",command.IdempotencyKey));

        if (!string.IsNullOrEmpty(command.IdempotencyKey))
        {
            var insertedSubmission = await connection.ExecuteAsync(new CommandDefinition("""
                INSERT INTO Kj_JobSubmissions(SpecId,IdempotencyKey,BatchId,PayloadHash,CreatedAt)
                VALUES(@SpecId,@IdempotencyKey,@BatchId,@PayloadHash,clock_timestamp())
                ON CONFLICT(SpecId,IdempotencyKey) DO NOTHING;
                """,new
            {
                SpecId=spec.Id,command.IdempotencyKey,BatchId=batchId,command.PayloadHash
            },transaction,cancellationToken:cancellationToken));
            if (insertedSubmission==0)
            {
                var existing=await connection.QuerySingleAsync<ExistingSubmission>(new CommandDefinition("""
                    SELECT BatchId,PayloadHash FROM Kj_JobSubmissions
                    WHERE SpecId=@SpecId AND IdempotencyKey=@IdempotencyKey FOR SHARE;
                    """,new{SpecId=spec.Id,command.IdempotencyKey},transaction,cancellationToken:cancellationToken));
                if (!CryptographicOperations.FixedTimeEquals(existing.PayloadHash,command.PayloadHash))
                    throw new InvalidOperationException("Idempotency key already exists with a different payload.");
                var ids=(await connection.QueryAsync<string>(new CommandDefinition(
                    "SELECT Id FROM Kj_JobRuns WHERE BatchId=@BatchId ORDER BY ShardIndex,Id",
                    new{existing.BatchId},transaction,cancellationToken:cancellationToken))).AsList();
                await transaction.CommitAsync(cancellationToken);
                return new JobSubmissionResult{BatchId=existing.BatchId,RunIds=ids,IsDuplicate=true};
            }
        }

        var targets = spec.ExecuteModel==ExecuteModel.Broadcast
            ? await GetBroadcastTargetsAsync(connection,transaction,spec,command.PayloadSchemaVersion,cancellationToken)
            : new List<BroadcastTarget>();
        if (targets.Count>MaxShards)
            throw new InvalidOperationException($"Broadcast target count exceeds {MaxShards}.");
        var runCount=spec.ExecuteModel switch
        {
            ExecuteModel.Standalone=>1,
            ExecuteModel.Sharding=>Math.Clamp(spec.TotalShards,1,MaxShards),
            ExecuteModel.Broadcast=>targets.Count,
            _=>throw new InvalidOperationException($"Unsupported execution model {(int)spec.ExecuteModel}.")
        };

        if (runCount==0)
        {
            await transaction.CommitAsync(cancellationToken);
            return new JobSubmissionResult{BatchId=batchId,RunIds=Array.Empty<string>()};
        }

        await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO Kj_JobPayloads(BatchId,PayloadJson,PayloadHash,CreatedAt)
            VALUES(@BatchId,convert_from(@PayloadUtf8,'UTF8')::jsonb,@PayloadHash,clock_timestamp());
            """,new{BatchId=batchId,command.PayloadUtf8,command.PayloadHash},
            transaction,cancellationToken:cancellationToken));

        var rows=new SubmissionRun[runCount];
        for(var i=0;i<runCount;i++)
        {
            var target=spec.ExecuteModel==ExecuteModel.Broadcast?targets[i]:null;
            rows[i]=new SubmissionRun
            {
                Id=Hash128(string.Concat(batchId,"|",i.ToString(CultureInfo.InvariantCulture),"|",
                    target?.WorkerId??string.Empty,"|",target?.SessionEpoch.ToString(CultureInfo.InvariantCulture)??string.Empty)),
                ShardIndex=i,
                PinnedWorkerId=target?.WorkerId,
                PinnedSessionEpoch=target?.SessionEpoch
            };
        }
        await InsertRunsAsync(connection,transaction,spec,command,batchId,rows,cancellationToken);
        await PostgreSqlQueueSignal.NotifyAsync(connection,transaction,cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new JobSubmissionResult{BatchId=batchId,RunIds=rows.Select(static x=>x.Id).ToArray()};
    }

    public Task<bool> CancelRunAsync(string runId,string reason,
        CancellationToken cancellationToken)=>CancelOneAsync("Id",runId,reason,cancellationToken);

    public async Task<int> CancelBatchAsync(string batchId,string reason,
        CancellationToken cancellationToken)=>await CancelAsync("BatchId",batchId,reason,cancellationToken);

    private async Task<bool> CancelOneAsync(string column,string value,string reason,
        CancellationToken cancellationToken)=>await CancelAsync(column,value,reason,cancellationToken)>0;

    private async Task<int> CancelAsync(string column,string value,string reason,
        CancellationToken cancellationToken)
    {
        var sql=$"""
            UPDATE Kj_JobRuns SET CancelRequestedAt=COALESCE(CancelRequestedAt,clock_timestamp()),
                Status=CASE WHEN Status IN (@Pending,@Assigned) THEN @Canceled ELSE Status END,
                EndTime=CASE WHEN Status IN (@Pending,@Assigned) THEN clock_timestamp() ELSE EndTime END,
                ResultMsg=CASE WHEN Status IN (@Pending,@Assigned) THEN @Reason ELSE ResultMsg END
            WHERE {column}=@Value AND Status IN (@Pending,@Assigned,@Running);
            """;
        await using var connection=await _dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteAsync(new CommandDefinition(sql,new
        {
            Value=value,Reason=reason,Pending=(int)JobStatus.Pending,
            Assigned=(int)JobStatus.Assigned,Running=(int)JobStatus.Running,Canceled=(int)JobStatus.Canceled
        },cancellationToken:cancellationToken));
    }

    private static async Task InsertRunsAsync(NpgsqlConnection connection,NpgsqlTransaction transaction,
        SubmissionSpec spec,JobSubmissionCommand command,string batchId,SubmissionRun[] runs,
        CancellationToken cancellationToken)
    {
        var ids=runs.Select(static x=>x.Id).ToArray();
        var shards=runs.Select(static x=>x.ShardIndex).ToArray();
        var workers=runs.Select(static x=>x.PinnedWorkerId??string.Empty).ToArray();
        var epochs=runs.Select(static x=>x.PinnedSessionEpoch??0).ToArray();
        const string sql="""
            WITH input AS (
                SELECT * FROM unnest(@Ids::text[],@Shards::integer[],@Workers::text[],@Epochs::bigint[])
                    AS x(Id,ShardIndex,PinnedWorkerId,PinnedSessionEpoch)
            ) INSERT INTO Kj_JobRuns
                (Id,SpecId,BatchId,ShardIndex,BatchSize,Status,TargetNodeId,CreatedAt,ResultMsg,RowVersion,
                 Attempt,LeaseToken,WorkerSessionEpoch,AvailableAt,ScheduledAt,QueueName,Priority,PayloadJson,
                 IdempotencyKey,JobType,TimeoutSeconds,MaxRetries,NodeSelector,RequiredHandlerVersion,
                 PayloadSchemaVersion,PinnedWorkerId,PinnedSessionEpoch)
            SELECT Id,@SpecId,@BatchId,ShardIndex,@BatchSize,@Pending,NULL,clock_timestamp(),'','',0,0,0,
                   COALESCE(@AvailableAt,clock_timestamp()),NULL,@QueueName,@Priority,'{}'::jsonb,@IdempotencyKey,@JobType,
                   @TimeoutSeconds,@MaxRetries,@NodeSelector::jsonb,@RequiredHandlerVersion,
                   @PayloadSchemaVersion,NULLIF(PinnedWorkerId,''),NULLIF(PinnedSessionEpoch,0)
            FROM input;
            """;
        await connection.ExecuteAsync(new CommandDefinition(sql,new
        {
            Ids=ids,Shards=shards,Workers=workers,Epochs=epochs,SpecId=spec.Id,BatchId=batchId,
            BatchSize=runs.Length,Pending=(int)JobStatus.Pending,command.AvailableAt,
            QueueName=string.IsNullOrWhiteSpace(command.QueueName)?spec.QueueName:command.QueueName,
            Priority=command.Priority??spec.Priority,command.IdempotencyKey,spec.JobType,
            spec.TimeoutSeconds,spec.MaxRetries,NodeSelector=spec.NodeSelectorJson,
            spec.RequiredHandlerVersion,command.PayloadSchemaVersion
        },transaction,cancellationToken:cancellationToken));
    }

    private static async Task<List<BroadcastTarget>> GetBroadcastTargetsAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction,SubmissionSpec spec,int payloadSchema,
        CancellationToken cancellationToken)
    {
        const string sql="""
            SELECT w.Id AS WorkerId,w.SessionEpoch FROM Kj_WorkerNodes w
            JOIN Kj_WorkerCapabilities c ON c.WorkerId=w.Id AND c.SessionEpoch=w.SessionEpoch
                AND c.JobType=@JobType
            WHERE w.IsOffline=FALSE AND w.Draining=FALSE AND w.LastHeartbeat>=clock_timestamp()-INTERVAL '30 seconds'
              AND COALESCE(w.Labels,'{}'::jsonb) @> @NodeSelector::jsonb
              AND (@RequiredHandlerVersion='' OR c.HandlerVersion=@RequiredHandlerVersion)
              AND c.PayloadSchemaVersion>=@PayloadSchema
            ORDER BY w.Id,w.SessionEpoch LIMIT @Limit;
            """;
        return (await connection.QueryAsync<BroadcastTarget>(new CommandDefinition(sql,new
        {
            spec.JobType,NodeSelector=spec.NodeSelectorJson,spec.RequiredHandlerVersion,
            PayloadSchema=payloadSchema,Limit=MaxShards+1
        },transaction,cancellationToken:cancellationToken))).AsList();
    }

    private static string Hash128(string input)=>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(input)).AsSpan(0,16)).ToLowerInvariant();

    private sealed class SubmissionSpec
    {
        public string Id{get;init;}=string.Empty; public string Name{get;init;}=string.Empty;
        public string JobType{get;init;}=string.Empty; public string NodeSelectorJson{get;init;}="{}";
        public ExecuteModel ExecuteModel{get;init;} public int TotalShards{get;init;}
        public int TimeoutSeconds{get;init;} public int MaxRetries{get;init;}
        public string QueueName{get;init;}="default"; public int Priority{get;init;}
        public string RequiredHandlerVersion{get;init;}=string.Empty; public int PayloadSchemaVersion{get;init;}=1;
    }
    private sealed class ExistingSubmission{public string BatchId{get;init;}=string.Empty;public byte[] PayloadHash{get;init;}=Array.Empty<byte>();}
    private sealed class BroadcastTarget{public string WorkerId{get;init;}=string.Empty;public long SessionEpoch{get;init;}}
    private sealed class SubmissionRun{public string Id{get;init;}=string.Empty;public int ShardIndex{get;init;}public string? PinnedWorkerId{get;init;}public long? PinnedSessionEpoch{get;init;}}
}
