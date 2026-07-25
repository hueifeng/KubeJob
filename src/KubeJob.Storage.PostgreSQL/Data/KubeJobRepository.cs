using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Dapper;
using KubeJob.Core.Domain;
using KubeJob.Core.Enums;
using KubeJob.Server.Data;
using Npgsql;
using Microsoft.Extensions.Configuration;

namespace KubeJob.Storage.PostgreSQL.Data
{
    public class KubeJobRepository : IKubeJobRepository
    {
        private readonly string _connectionString;

        public KubeJobRepository(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            _connectionString = connectionString;
        }

        private IDbConnection CreateConnection() => new NpgsqlConnection(_connectionString);

        // JobSpec Operations
        public async Task<List<JobSpec>> GetAllSpecsAsync()
        {
            using var conn = CreateConnection();
            return (await conn.QueryAsync<JobSpec>("SELECT * FROM Kj_JobSpecs")).ToList();
        }

        public async Task<int> GetSpecsCountAsync()
        {
            using var conn = CreateConnection();
            return await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Kj_JobSpecs");
        }

        public async Task<List<JobSpec>> GetSpecsPagedAsync(int limit, int offset)
        {
            using var conn = CreateConnection();
            return (await conn.QueryAsync<JobSpec>("SELECT * FROM Kj_JobSpecs ORDER BY Id LIMIT @Limit OFFSET @Offset", new { Limit = limit, Offset = offset })).ToList();
        }
        
        public async Task<JobSpec?> GetSpecAsync(string id)
        {
            using var conn = CreateConnection();
            var sql = "SELECT * FROM Kj_JobSpecs WHERE Id = @Id";
            return await conn.QueryFirstOrDefaultAsync<JobSpec>(sql, new { Id = id });
        }

        public async Task<JobSpec?> GetSpecByNameAsync(string name)
        {
            using var conn = CreateConnection();
            var sql = "SELECT * FROM Kj_JobSpecs WHERE Name = @Name LIMIT 1";
            return await conn.QueryFirstOrDefaultAsync<JobSpec>(sql, new { Name = name });
        }

        public async Task UpsertSpecAsync(JobSpec spec)
        {
            using var conn = CreateConnection();
            var sql = @"
                INSERT INTO Kj_JobSpecs (Id, Name, JobType, CronExpression, NodeSelector, ExecuteModel, TotalShards, NextRunTime, IsDisabled, TimeoutSeconds, MaxRetries, ConcurrencyPolicy, SuccessfulJobsHistoryLimit, FailedJobsHistoryLimit)
                VALUES (@Id, @Name, @JobType, @CronExpression, @NodeSelector::jsonb, @ExecuteModel, @TotalShards, @NextRunTime, @IsDisabled, @TimeoutSeconds, @MaxRetries, @ConcurrencyPolicy, @SuccessfulJobsHistoryLimit, @FailedJobsHistoryLimit)
                ON CONFLICT (Id) DO UPDATE SET 
                    Name = EXCLUDED.Name,
                    JobType = EXCLUDED.JobType,
                    CronExpression = EXCLUDED.CronExpression,
                    NodeSelector = EXCLUDED.NodeSelector,
                    ExecuteModel = EXCLUDED.ExecuteModel,
                    TotalShards = EXCLUDED.TotalShards,
                    TimeoutSeconds = EXCLUDED.TimeoutSeconds,
                    MaxRetries = EXCLUDED.MaxRetries,
                    ConcurrencyPolicy = EXCLUDED.ConcurrencyPolicy,
                    SuccessfulJobsHistoryLimit = EXCLUDED.SuccessfulJobsHistoryLimit,
                    FailedJobsHistoryLimit = EXCLUDED.FailedJobsHistoryLimit;
            ";
            await conn.ExecuteAsync(sql, spec);
        }

        public async Task UpdateNextRunTimeAsync(string specId, DateTime? nextRunTime)
        {
            using var conn = CreateConnection();
            await conn.ExecuteAsync("UPDATE Kj_JobSpecs SET NextRunTime = @NextRunTime WHERE Id = @Id", new { Id = specId, NextRunTime = nextRunTime });
        }

        public async Task UpdateSpecStatusAsync(string specId, bool isDisabled)
        {
            using var conn = CreateConnection();
            await conn.ExecuteAsync("UPDATE Kj_JobSpecs SET IsDisabled = @IsDisabled WHERE Id = @Id", new { Id = specId, IsDisabled = isDisabled });
        }

        // JobRun Operations
        public async Task<bool> InsertJobRunAsync(JobRun run)
        {
            using var conn = CreateConnection();
            var sql = @"
                INSERT INTO Kj_JobRuns (Id, SpecId, BatchId, ShardIndex, Status, TargetNodeId, CreatedAt, StartTime, EndTime, ResultMsg, RowVersion)
                VALUES (@Id, @SpecId, @BatchId, @ShardIndex, @Status, @TargetNodeId, @CreatedAt, @StartTime, @EndTime, @ResultMsg, @RowVersion)
            ";
            try
            {
                await conn.ExecuteAsync(sql, new {
                    run.Id, run.SpecId, run.BatchId, run.ShardIndex, Status = (int)run.Status, 
                    run.TargetNodeId, run.CreatedAt, run.StartTime, run.EndTime, run.ResultMsg,
                    RowVersion = Guid.NewGuid().ToString()
                });
                return true;
            }
            catch (PostgresException ex) when (ex.SqlState == "23505") // Unique violation
            {
                return false; 
            }
        }

        public async Task<List<JobRun>> GetPendingRunsAsync()
        {
            using var conn = CreateConnection();
            return (await conn.QueryAsync<JobRun>(
                "SELECT * FROM Kj_JobRuns WHERE Status = @Status ORDER BY CreatedAt ASC",
                new { Status = (int)JobStatus.Pending })).ToList();
        }

        public async Task<bool> HasActiveRunsForSpecAsync(string specId)
        {
            using var conn = CreateConnection();
            var activeStatuses = new[] { (int)JobStatus.Pending, (int)JobStatus.Assigned, (int)JobStatus.Running };
            var count = await conn.ExecuteScalarAsync<int>(
                "SELECT COUNT(1) FROM Kj_JobRuns WHERE SpecId = @SpecId AND Status = ANY(@Statuses)",
                new { SpecId = specId, Statuses = activeStatuses });
            return count > 0;
        }

        public async Task CancelActiveRunsForSpecAsync(string specId, string reason)
        {
            using var conn = CreateConnection();
            var sql = @"
                UPDATE Kj_JobRuns 
                SET Status = @CanceledStatus, ResultMsg = @Reason, EndTime = @Now
                WHERE SpecId = @SpecId AND Status = ANY(@ActiveStatuses)
            ";
            await conn.ExecuteAsync(sql, new
            {
                SpecId = specId,
                Reason = reason,
                Now = DateTime.UtcNow,
                CanceledStatus = (int)JobStatus.Canceled,
                ActiveStatuses = new[] { (int)JobStatus.Pending, (int)JobStatus.Assigned, (int)JobStatus.Running }
            });
        }

        public async Task<bool> AssignRunAsync(string runId, string targetNodeId, string oldRowVersion)
        {
            using var conn = CreateConnection();
            var newRowVersion = Guid.NewGuid().ToString();
            var sql = @"
                UPDATE Kj_JobRuns
                SET TargetNodeId = @TargetNodeId, Status = @AssignedStatus, RowVersion = @NewRowVersion
                WHERE Id = @Id AND Status = @PendingStatus AND RowVersion = @OldRowVersion
            ";
            var affected = await conn.ExecuteAsync(sql, new
            {
                TargetNodeId = targetNodeId,
                NewRowVersion = newRowVersion,
                Id = runId,
                OldRowVersion = oldRowVersion,
                AssignedStatus = (int)JobStatus.Assigned,
                PendingStatus = (int)JobStatus.Pending
            });
            return affected > 0;
        }

        public async Task<List<JobRun>> GetAssignedRunsForNodeAsync(string nodeId)
        {
            using var conn = CreateConnection();
            var sql = @"
                SELECT r.*, s.JobType, s.TimeoutSeconds
                FROM Kj_JobRuns r
                JOIN Kj_JobSpecs s ON r.SpecId = s.Id
                WHERE r.Status = @AssignedStatus AND r.TargetNodeId = @NodeId
                ORDER BY r.CreatedAt ASC
            ";
            return (await conn.QueryAsync<JobRun>(sql, new { NodeId = nodeId, AssignedStatus = (int)JobStatus.Assigned })).ToList();
        }

        public async Task MarkRunStatusAsync(string runId, JobStatus status, string resultMsg = "", DateTime? startTime = null, DateTime? endTime = null)
        {
            using var conn = CreateConnection();
            var sql = "UPDATE Kj_JobRuns SET Status = @Status, ResultMsg = @ResultMsg";
            if (startTime.HasValue) sql += ", StartTime = @StartTime";
            if (endTime.HasValue) sql += ", EndTime = @EndTime";
            sql += " WHERE Id = @Id";
            
            await conn.ExecuteAsync(sql, new { Id = runId, Status = (int)status, ResultMsg = resultMsg, StartTime = startTime, EndTime = endTime });
        }
        
        public async Task<List<JobRun>> GetRecentRunsAsync(int limit = 50)
        {
            using var conn = CreateConnection();
            return (await conn.QueryAsync<JobRun>("SELECT * FROM Kj_JobRuns ORDER BY CreatedAt DESC LIMIT @Limit", new { Limit = limit })).ToList();
        }

        public async Task<int> GetRunsCountAsync(JobStatus? status = null)
        {
            using var conn = CreateConnection();
            if (status.HasValue)
            {
                return await conn.ExecuteScalarAsync<int>(
                    "SELECT COUNT(*) FROM Kj_JobRuns WHERE Status = @Status",
                    new { Status = (int)status.Value });
            }

            return await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Kj_JobRuns");
        }

        public async Task<JobRun?> GetJobRunAsync(string runId)
        {
            using var conn = CreateConnection();
            var sql = "SELECT * FROM Kj_JobRuns WHERE Id = @Id LIMIT 1";
            return await conn.QueryFirstOrDefaultAsync<JobRun>(sql, new { Id = runId });
        }

        public async Task<int> GetFailedRunsCountAsync(string batchId, int shardIndex)
        {
            using var conn = CreateConnection();
            var sql = "SELECT COUNT(*) FROM Kj_JobRuns WHERE BatchId = @BatchId AND ShardIndex = @ShardIndex AND Status = @Status";
            return await conn.ExecuteScalarAsync<int>(sql, new { BatchId = batchId, ShardIndex = shardIndex, Status = (int)JobStatus.Failed });
        }

        public async Task<List<JobRun>> GetRunsPagedAsync(int limit, int offset, JobStatus? status = null)
        {
            using var conn = CreateConnection();
            if (status.HasValue)
            {
                return (await conn.QueryAsync<JobRun>(
                    "SELECT * FROM Kj_JobRuns WHERE Status = @Status ORDER BY CreatedAt DESC LIMIT @Limit OFFSET @Offset",
                    new { Status = (int)status.Value, Limit = limit, Offset = offset })).ToList();
            }

            return (await conn.QueryAsync<JobRun>(
                "SELECT * FROM Kj_JobRuns ORDER BY CreatedAt DESC LIMIT @Limit OFFSET @Offset",
                new { Limit = limit, Offset = offset })).ToList();
        }

        public async Task<Dictionary<JobStatus, int>> GetRunStatusCountsAsync()
        {
            using var conn = CreateConnection();
            var rows = await conn.QueryAsync<(int Status, int Count)>(
                "SELECT Status, COUNT(*) AS Count FROM Kj_JobRuns GROUP BY Status");

            return rows.ToDictionary(r => (JobStatus)r.Status, r => r.Count);
        }

        public async Task<List<JobRunHistogramBucket>> GetRunHistogramAsync(DateTime sinceUtc)
        {
            using var conn = CreateConnection();
            var sql = @"
                SELECT date_trunc('hour', CreatedAt) AS BucketUtc, Status, COUNT(*) AS Count
                FROM Kj_JobRuns
                WHERE CreatedAt >= @SinceUtc
                GROUP BY 1, 2
                ORDER BY 1
            ";
            return (await conn.QueryAsync<JobRunHistogramBucket>(sql, new { SinceUtc = sinceUtc })).ToList();
        }

        public async Task<int> DeleteOldRunsAsync(DateTime cutoffTime)
        {
            using var conn = CreateConnection();
            return await conn.ExecuteAsync("DELETE FROM Kj_JobRuns WHERE CreatedAt < @CutoffTime", new { CutoffTime = cutoffTime });
        }

        public async Task<int> CleanupHistoryBySpecLimitsAsync()
        {
            using var conn = CreateConnection();
            var sql = @"
                WITH RankedRuns AS (
                    SELECT 
                        r.Id, 
                        r.SpecId, 
                        r.Status,
                        s.SuccessfulJobsHistoryLimit,
                        s.FailedJobsHistoryLimit,
                        ROW_NUMBER() OVER(PARTITION BY r.SpecId, r.Status ORDER BY r.CreatedAt DESC) as rn
                    FROM Kj_JobRuns r
                    JOIN Kj_JobSpecs s ON r.SpecId = s.Id
                    WHERE r.Status IN (@SucceededStatus, @FailedStatus)
                )
                DELETE FROM Kj_JobRuns
                WHERE Id IN (
                    SELECT Id FROM RankedRuns 
                    WHERE (Status = @SucceededStatus AND rn > SuccessfulJobsHistoryLimit)
                       OR (Status = @FailedStatus AND rn > FailedJobsHistoryLimit)
                );
            ";
            return await conn.ExecuteAsync(sql, new
            {
                SucceededStatus = (int)JobStatus.Succeeded,
                FailedStatus = (int)JobStatus.Failed
            });
        }

        // WorkerNode Operations
        public async Task UpsertWorkerNodeAsync(WorkerNode node)
        {
            using var conn = CreateConnection();
            var sql = @"
                INSERT INTO Kj_WorkerNodes (Id, IpAddress, Labels, LastHeartbeat, CurrentLoad, MaxCapacity, IsOffline)
                VALUES (@Id, @IpAddress, @Labels::jsonb, @LastHeartbeat, @CurrentLoad, @MaxCapacity, @IsOffline)
                ON CONFLICT (Id) DO UPDATE SET 
                    IpAddress = EXCLUDED.IpAddress,
                    Labels = EXCLUDED.Labels,
                    LastHeartbeat = EXCLUDED.LastHeartbeat,
                    CurrentLoad = EXCLUDED.CurrentLoad,
                    MaxCapacity = EXCLUDED.MaxCapacity,
                    IsOffline = false;
            ";
            await conn.ExecuteAsync(sql, new {
                node.Id, node.IpAddress, Labels = node.Labels, node.LastHeartbeat, node.CurrentLoad, node.MaxCapacity, node.IsOffline
            });
        }

        public async Task<List<WorkerNode>> GetActiveNodesAsync()
        {
            using var conn = CreateConnection();
            return (await conn.QueryAsync<WorkerNode>("SELECT * FROM Kj_WorkerNodes WHERE IsOffline = false")).ToList();
        }
        
        public async Task<List<WorkerNode>> GetAllNodesAsync()
        {
            using var conn = CreateConnection();
            return (await conn.QueryAsync<WorkerNode>("SELECT * FROM Kj_WorkerNodes")).ToList();
        }

        public async Task MarkNodesOfflineAsync(DateTime cutoffTime)
        {
            using var conn = CreateConnection();
            await conn.ExecuteAsync("UPDATE Kj_WorkerNodes SET IsOffline = true WHERE LastHeartbeat < @CutoffTime", new { CutoffTime = cutoffTime });
        }

        public async Task ResetOfflineNodeRunsAsync()
        {
            using var conn = CreateConnection();
            var sql = @"
                UPDATE Kj_JobRuns 
                SET Status = @PendingStatus, TargetNodeId = NULL, RowVersion = @NewRowVersion
                WHERE Status = ANY(@InFlightStatuses) AND TargetNodeId IN (SELECT Id FROM Kj_WorkerNodes WHERE IsOffline = true)
            ";
            await conn.ExecuteAsync(sql, new
            {
                NewRowVersion = Guid.NewGuid().ToString(),
                PendingStatus = (int)JobStatus.Pending,
                InFlightStatuses = new[] { (int)JobStatus.Assigned, (int)JobStatus.Running }
            });
        }

        public async Task DeleteNodeAsync(string nodeId)
        {
            using var conn = CreateConnection();
            await conn.ExecuteAsync("DELETE FROM Kj_WorkerNodes WHERE Id = @Id", new { Id = nodeId });
        }
    }
}
