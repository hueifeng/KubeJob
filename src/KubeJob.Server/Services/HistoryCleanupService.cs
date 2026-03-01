using System;
using System.Threading;
using System.Threading.Tasks;
using KubeJob.Server.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KubeJob.Server.Services
{
    public class HistoryCleanupService : BackgroundService
    {
        private readonly IKubeJobRepository _repository;
        private readonly IKubeJobLockProvider _lockProvider;
        private readonly IServerIdentity _serverIdentity;
        private readonly ILogger<HistoryCleanupService> _logger;
        private const string LockName = "kubejob-history-cleanup";
        private readonly TimeSpan _lockExpiry = TimeSpan.FromMinutes(5);

        public HistoryCleanupService(
            IKubeJobRepository repository, 
            IKubeJobLockProvider lockProvider,
            IServerIdentity serverIdentity,
            ILogger<HistoryCleanupService> logger)
        {
            _repository = repository;
            _lockProvider = lockProvider;
            _serverIdentity = serverIdentity;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Run cleanup loop every 1 hour
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    bool isLeader = await _lockProvider.TryAcquireLockAsync(LockName, _serverIdentity.ServerId, _lockExpiry, stoppingToken);

                    if (!isLeader)
                    {
                        await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
                        continue;
                    }

                    _logger.LogInformation("Running history cleanup...");
                    // 1. Keep history for 7 days (Absolute fallback)
                    var cutoffTime = DateTime.UtcNow.AddDays(-7);
                    var deletedCount = await _repository.DeleteOldRunsAsync(cutoffTime);
                    
                    // 2. K8s-style history limits (SuccessfulJobsHistoryLimit / FailedJobsHistoryLimit)
                    var k8sDeletedCount = await _repository.CleanupHistoryBySpecLimitsAsync();
                    
                    if (deletedCount > 0 || k8sDeletedCount > 0)
                    {
                        _logger.LogInformation($"Cleaned up {deletedCount} old job runs based on time, and {k8sDeletedCount} based on history limits.");
                    }

                    // Release lock so it can be picked up safely later
                    await _lockProvider.ReleaseLockAsync(LockName, _serverIdentity.ServerId, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in HistoryCleanupService");
                }

                // Wait 1 hour before next cleanup check
                await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
            }
        }
    }
}
