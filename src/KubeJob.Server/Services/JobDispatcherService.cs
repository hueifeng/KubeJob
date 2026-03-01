using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using KubeJob.Server.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KubeJob.Server.Services
{
    public class JobDispatcherService : BackgroundService
    {
        private readonly IKubeJobRepository _repository;
        private readonly IKubeJobLockProvider _lockProvider;
        private readonly IServerIdentity _serverIdentity;
        private readonly ILogger<JobDispatcherService> _logger;
        private const string LockName = "kubejob-dispatcher";
        private readonly TimeSpan _lockExpiry = TimeSpan.FromSeconds(15);

        public JobDispatcherService(
            IKubeJobRepository repository, 
            IKubeJobLockProvider lockProvider,
            IServerIdentity serverIdentity,
            ILogger<JobDispatcherService> logger)
        {
            _repository = repository;
            _lockProvider = lockProvider;
            _serverIdentity = serverIdentity;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    bool isLeader = await _lockProvider.TryAcquireLockAsync(LockName, _serverIdentity.ServerId, _lockExpiry, stoppingToken);

                    if (!isLeader)
                    {
                        await Task.Delay(2000, stoppingToken);
                        continue;
                    }

                    var pendingRuns = await _repository.GetPendingRunsAsync();
                    if (pendingRuns.Any())
                    {
                        var nodes = await _repository.GetActiveNodesAsync();
                        var specsCache = (await _repository.GetAllSpecsAsync()).ToDictionary(x => x.Id);

                        foreach (var run in pendingRuns)
                        {
                            if (!specsCache.TryGetValue(run.SpecId, out var spec)) continue;

                            var targetNode = MatchNode(spec, nodes);
                            if (targetNode != null)
                            {
                                var assigned = await _repository.AssignRunAsync(run.Id, targetNode.Id, run.RowVersion ?? string.Empty);
                                if (assigned)
                                {
                                    _logger.LogInformation($"Assigned JobRun {run.Id} to Node {targetNode.Id}");
                                    // Naive local decrement for load balancing in current iteration
                                    targetNode.CurrentLoad++;
                                }
                            }
                        }
                    }

                    await _lockProvider.RenewLockAsync(LockName, _serverIdentity.ServerId, _lockExpiry, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in JobDispatcherService");
                }

                await Task.Delay(1000, stoppingToken);
            }
        }

        private Core.Domain.WorkerNode? MatchNode(Core.Domain.JobSpec spec, System.Collections.Generic.List<Core.Domain.WorkerNode> nodes)
        {
            // Parse node selector
            var requiredLabels = JsonSerializer.Deserialize<System.Collections.Generic.Dictionary<string, string>>(spec.NodeSelector) ?? new();
            
            var availableNodes = nodes.Where(n => 
            {
                if (n.CurrentLoad >= n.MaxCapacity) return false;
                
                var nodeLabels = JsonSerializer.Deserialize<System.Collections.Generic.Dictionary<string, string>>(n.Labels) ?? new();
                // Check if all required labels exist in node labels
                foreach(var req in requiredLabels)
                {
                    if (!nodeLabels.TryGetValue(req.Key, out var val) || val != req.Value) return false;
                }
                return true;
            }).ToList();

            // Return node with least load
            return availableNodes.OrderBy(n => n.CurrentLoad).FirstOrDefault();
        }
    }
}
