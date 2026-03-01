using System;
using System.Threading;
using System.Threading.Tasks;
using KubeJob.Server.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KubeJob.Server.Services
{
    public class NodeHealthService : BackgroundService
    {
        private readonly IKubeJobRepository _repository;
        private readonly IKubeJobLockProvider _lockProvider;
        private readonly IServerIdentity _serverIdentity;
        private readonly ILogger<NodeHealthService> _logger;
        private const string LockName = "kubejob-health-check";
        private readonly TimeSpan _lockExpiry = TimeSpan.FromSeconds(30);

        public NodeHealthService(
            IKubeJobRepository repository, 
            IKubeJobLockProvider lockProvider,
            IServerIdentity serverIdentity,
            ILogger<NodeHealthService> logger)
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
                        await Task.Delay(5000, stoppingToken);
                        continue;
                    }

                    var cutoffTime = DateTime.UtcNow.AddSeconds(-30);
                    await _repository.MarkNodesOfflineAsync(cutoffTime);
                    await _repository.ResetOfflineNodeRunsAsync();

                    await _lockProvider.RenewLockAsync(LockName, _serverIdentity.ServerId, _lockExpiry, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in NodeHealthService");
                }

                await Task.Delay(15000, stoppingToken); // Check every 15s
            }
        }
    }
}
