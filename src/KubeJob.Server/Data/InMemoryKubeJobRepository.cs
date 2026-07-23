using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KubeJob.Core.Domain;
using KubeJob.Core.Enums;

namespace KubeJob.Server.Data
{
    public class InMemoryKubeJobRepository : IKubeJobRepository
    {
        private readonly ConcurrentDictionary<string, JobSpec> _specs = new();
        private readonly ConcurrentDictionary<string, JobRun> _runs = new();
        private readonly ConcurrentDictionary<string, WorkerNode> _nodes = new();

        public Task<List<JobSpec>> GetAllSpecsAsync()
        {
            return Task.FromResult(_specs.Values.ToList());
        }

        public Task<int> GetSpecsCountAsync()
        {
            return Task.FromResult(_specs.Count);
        }

        public Task<List<JobSpec>> GetSpecsPagedAsync(int limit, int offset)
        {
            var paged = _specs.Values.OrderBy(x => x.Name).Skip(offset).Take(limit).ToList();
            return Task.FromResult(paged);
        }

        public Task<JobSpec?> GetSpecAsync(string id)
        {
            _specs.TryGetValue(id, out var spec);
            return Task.FromResult(spec);
        }

        public Task<JobSpec?> GetSpecByNameAsync(string name)
        {
            var spec = _specs.Values.FirstOrDefault(s => s.Name == name);
            return Task.FromResult(spec);
        }

        public Task UpsertSpecAsync(JobSpec spec)
        {
            var existing = GetSpecByNameAsync(spec.Name).Result;
            if (existing != null)
            {
                spec.Id = existing.Id;
            }
            _specs[spec.Id] = spec;
            return Task.CompletedTask;
        }

        public Task UpdateNextRunTimeAsync(string specId, DateTime? nextRunTime)
        {
            if (_specs.TryGetValue(specId, out var spec))
            {
                spec.NextRunTime = nextRunTime;
            }
            return Task.CompletedTask;
        }

        public Task UpdateSpecStatusAsync(string specId, bool isDisabled)
        {
            if (_specs.TryGetValue(specId, out var spec))
            {
                spec.IsDisabled = isDisabled;
            }
            return Task.CompletedTask;
        }

        public Task<bool> InsertJobRunAsync(JobRun run)
        {
            return Task.FromResult(_runs.TryAdd(run.Id, run));
        }

        public Task<List<JobRun>> GetPendingRunsAsync(int limit = 100)
        {
            var pending = _runs.Values.Where(r => r.Status == JobStatus.Pending).OrderBy(r => r.CreatedAt).ThenBy(r => r.Id).Take(Math.Clamp(limit, 1, 1000)).ToList();
            return Task.FromResult(pending);
        }

        public Task<bool> HasActiveRunsForSpecAsync(string specId)
        {
            var hasActive = _runs.Values.Any(r => r.SpecId == specId && (r.Status == JobStatus.Pending || r.Status == JobStatus.Assigned || r.Status == JobStatus.Running));
            return Task.FromResult(hasActive);
        }

        public Task CancelActiveRunsForSpecAsync(string specId, string reason)
        {
            var activeRuns = _runs.Values.Where(r => r.SpecId == specId && (r.Status == JobStatus.Pending || r.Status == JobStatus.Assigned || r.Status == JobStatus.Running));
            foreach (var run in activeRuns)
            {
                run.Status = JobStatus.Canceled;
                run.ResultMsg = reason;
                run.EndTime = DateTime.UtcNow;
                run.RowVersion = Guid.NewGuid().ToString();
            }
            return Task.CompletedTask;
        }

        public Task<bool> AssignRunAsync(string runId, string targetNodeId, string oldRowVersion)
        {
            if (_runs.TryGetValue(runId, out var run) && run.Status == JobStatus.Pending && run.RowVersion == oldRowVersion)
            {
                run.TargetNodeId = targetNodeId;
                run.Status = JobStatus.Running;
                run.RowVersion = Guid.NewGuid().ToString();
                return Task.FromResult(true);
            }
            return Task.FromResult(false);
        }

        public Task<List<JobRun>> GetAssignedRunsForNodeAsync(string nodeId, int limit = 10)
        {
            var assigned = _runs.Values
                .Where(r => r.Status == JobStatus.Assigned && r.TargetNodeId == nodeId)
                .Select(r => 
                {
                    // Copy and inject missing props that the UI expects via JOIN in Postgres
                    if (_specs.TryGetValue(r.SpecId, out var spec))
                    {
                        r.JobType = spec.JobType;
                        r.TimeoutSeconds = spec.TimeoutSeconds;
                    }
                    return r;
                })
                .Take(Math.Clamp(limit, 1, 1000)).ToList();
            return Task.FromResult(assigned);
        }

        public Task MarkRunStatusAsync(string runId, JobStatus status, string resultMsg = "", DateTime? startTime = null, DateTime? endTime = null, string? workerId = null, string? rowVersion = null)
        {
            if (_runs.TryGetValue(runId, out var run) &&
                (string.IsNullOrWhiteSpace(workerId) || (run.TargetNodeId == workerId && run.RowVersion == rowVersion && (run.Status == JobStatus.Assigned || run.Status == JobStatus.Running))))
            {
                run.Status = status;
                run.ResultMsg = resultMsg;
                if (startTime.HasValue) run.StartTime = startTime;
                if (endTime.HasValue) run.EndTime = endTime;
            }
            return Task.CompletedTask;
        }

        public Task<List<JobRun>> GetRecentRunsAsync(int limit = 50)
        {
            var recent = _runs.Values.OrderByDescending(r => r.CreatedAt).Take(limit).ToList();
            return Task.FromResult(recent);
        }

        public Task<int> GetRunsCountAsync()
        {
            return Task.FromResult(_runs.Count);
        }

        public Task<JobRun?> GetJobRunAsync(string runId)
        {
            _runs.TryGetValue(runId, out var run);
            return Task.FromResult(run);
        }

        public Task<int> GetFailedRunsCountAsync(string batchId, int shardIndex)
        {
            var count = _runs.Values.Count(r => r.BatchId == batchId && r.ShardIndex == shardIndex && r.Status == JobStatus.Failed);
            return Task.FromResult(count);
        }

        public Task<List<JobRun>> GetRunsPagedAsync(int limit, int offset)
        {
            var paged = _runs.Values.OrderByDescending(r => r.CreatedAt).Skip(offset).Take(limit).ToList();
            return Task.FromResult(paged);
        }

        public Task<int> DeleteOldRunsAsync(DateTime cutoffTime)
        {
            var toDelete = _runs.Values.Where(r => r.CreatedAt < cutoffTime).ToList();
            int count = 0;
            foreach (var run in toDelete)
            {
                if (_runs.TryRemove(run.Id, out _)) count++;
            }
            return Task.FromResult(count);
        }

        public Task<int> CleanupHistoryBySpecLimitsAsync()
        {
            int deleted = 0;
            foreach (var spec in _specs.Values)
            {
                var specRuns = _runs.Values.Where(r => r.SpecId == spec.Id).ToList();
                
                var successRuns = specRuns.Where(r => r.Status == JobStatus.Succeeded).OrderByDescending(r => r.CreatedAt).ToList();
                if (successRuns.Count > spec.SuccessfulJobsHistoryLimit)
                {
                    foreach (var run in successRuns.Skip(spec.SuccessfulJobsHistoryLimit))
                    {
                        if (_runs.TryRemove(run.Id, out _)) deleted++;
                    }
                }

                var failedRuns = specRuns.Where(r => r.Status == JobStatus.Failed).OrderByDescending(r => r.CreatedAt).ToList();
                if (failedRuns.Count > spec.FailedJobsHistoryLimit)
                {
                    foreach (var run in failedRuns.Skip(spec.FailedJobsHistoryLimit))
                    {
                        if (_runs.TryRemove(run.Id, out _)) deleted++;
                    }
                }
            }
            return Task.FromResult(deleted);
        }

        public Task UpsertWorkerNodeAsync(WorkerNode node)
        {
            if (_nodes.TryGetValue(node.Id, out var existing))
            {
                node.IsOffline = existing.IsOffline;
            }
            _nodes[node.Id] = node;
            return Task.CompletedTask;
        }

        public Task<List<WorkerNode>> GetActiveNodesAsync()
        {
            var active = _nodes.Values.Where(n => !n.IsOffline).ToList();
            return Task.FromResult(active);
        }

        public Task<List<WorkerNode>> GetAllNodesAsync()
        {
            return Task.FromResult(_nodes.Values.ToList());
        }

        public Task MarkNodesOfflineAsync(DateTime cutoffTime)
        {
            foreach (var node in _nodes.Values.Where(n => !n.IsOffline && n.LastHeartbeat < cutoffTime))
            {
                node.IsOffline = true;
            }
            return Task.CompletedTask;
        }

        public Task ResetOfflineNodeRunsAsync()
        {
            var offlineNodeIds = _nodes.Values.Where(n => n.IsOffline).Select(n => n.Id).ToHashSet();
            var runsToReset = _runs.Values.Where(r => (r.Status == JobStatus.Assigned || r.Status == JobStatus.Running) && r.TargetNodeId != null && offlineNodeIds.Contains(r.TargetNodeId)).ToList();
            foreach (var run in runsToReset)
            {
                run.Status = JobStatus.Pending;
                run.TargetNodeId = string.Empty;
                run.RowVersion = Guid.NewGuid().ToString();
            }
            return Task.CompletedTask;
        }

        public Task DeleteNodeAsync(string nodeId)
        {
            _nodes.TryRemove(nodeId, out _);
            return Task.CompletedTask;
        }
    }
}
