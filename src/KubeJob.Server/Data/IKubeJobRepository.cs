using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using KubeJob.Core.Domain;
using KubeJob.Core.Enums;

namespace KubeJob.Server.Data
{
    public interface IKubeJobRepository
    {
        Task<List<JobSpec>> GetAllSpecsAsync();
        Task<int> GetSpecsCountAsync();
        Task<List<JobSpec>> GetSpecsPagedAsync(int limit, int offset);
        Task<JobSpec?> GetSpecAsync(string id);
        Task<JobSpec?> GetSpecByNameAsync(string name);
        Task UpsertSpecAsync(JobSpec spec);
        Task UpdateNextRunTimeAsync(string specId, DateTime? nextRunTime);
        Task UpdateSpecStatusAsync(string specId, bool isDisabled);
        Task<bool> InsertJobRunAsync(JobRun run);
        Task<List<JobRun>> GetPendingRunsAsync();
        Task<bool> HasActiveRunsForSpecAsync(string specId);
        Task CancelActiveRunsForSpecAsync(string specId, string reason);
        Task<bool> AssignRunAsync(string runId, string targetNodeId, string oldRowVersion);
        Task<bool> TryTransitionRunStatusAsync(
            string runId,
            string targetNodeId,
            JobStatus expectedStatus,
            JobStatus newStatus,
            DateTime? startTime = null,
            DateTime? endTime = null);
        Task<List<JobRun>> GetAssignedRunsForNodeAsync(string nodeId);
        Task MarkRunStatusAsync(string runId, JobStatus status, string resultMsg = "", DateTime? startTime = null, DateTime? endTime = null);
        Task<List<JobRun>> GetRecentRunsAsync(int limit = 50);
        Task<int> GetRunsCountAsync();
        Task<JobRun?> GetJobRunAsync(string runId);
        Task<int> GetFailedRunsCountAsync(string batchId, int shardIndex);
        Task<List<JobRun>> GetRunsPagedAsync(int limit, int offset);
        Task<int> DeleteOldRunsAsync(DateTime cutoffTime);
        Task<int> CleanupHistoryBySpecLimitsAsync();
        Task UpsertWorkerNodeAsync(WorkerNode node);
        Task<List<WorkerNode>> GetActiveNodesAsync();
        Task<List<WorkerNode>> GetAllNodesAsync();
        Task MarkNodesOfflineAsync(DateTime cutoffTime);
        Task ResetOfflineNodeRunsAsync();
        Task DeleteNodeAsync(string nodeId);
    }
}
