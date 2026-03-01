using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cronos;
using KubeJob.Core.Domain;
using KubeJob.Server.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KubeJob.Server.Services
{
    public class CronSchedulerService : BackgroundService
    {
        private readonly IKubeJobRepository _repository;
        private readonly IKubeJobLockProvider _lockProvider;
        private readonly IServerIdentity _serverIdentity;
        private readonly ILogger<CronSchedulerService> _logger;
        private const string LockName = "kubejob-cron-scheduler";
        private readonly TimeSpan _lockExpiry = TimeSpan.FromSeconds(15);

        public CronSchedulerService(
            IKubeJobRepository repository, 
            IKubeJobLockProvider lockProvider,
            IServerIdentity serverIdentity,
            ILogger<CronSchedulerService> logger)
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
                        // Not the leader, wait and try again
                        await Task.Delay(2000, stoppingToken);
                        continue;
                    }

                    var specs = await _repository.GetAllSpecsAsync();
                    var now = DateTime.UtcNow;

                    foreach (var spec in specs)
                    {
                        if (spec.IsDisabled) continue;

                        if (spec.NextRunTime == null || spec.NextRunTime <= now)
                        {
                            // Time to trigger
                            var expression = CronExpression.Parse(spec.CronExpression, CronFormat.Standard);
                            var nextRun = expression.GetNextOccurrence(now);
                            
                            // Check Concurrency Policy
                            bool shouldSchedule = true;
                            if (spec.ConcurrencyPolicy != Core.Enums.ConcurrencyPolicy.Allow)
                            {
                                bool hasActiveRuns = await _repository.HasActiveRunsForSpecAsync(spec.Id);
                                if (hasActiveRuns)
                                {
                                    if (spec.ConcurrencyPolicy == Core.Enums.ConcurrencyPolicy.Forbid)
                                    {
                                        _logger.LogInformation($"Skipping schedule for Spec {spec.Id} because a previous run is still active (ConcurrencyPolicy=Forbid).");
                                        shouldSchedule = false;
                                    }
                                    else if (spec.ConcurrencyPolicy == Core.Enums.ConcurrencyPolicy.Replace)
                                    {
                                        _logger.LogInformation($"Canceling active runs for Spec {spec.Id} to replace with new run (ConcurrencyPolicy=Replace).");
                                        await _repository.CancelActiveRunsForSpecAsync(spec.Id, "Canceled by ConcurrencyPolicy: Replace");
                                    }
                                }
                            }

                            if (shouldSchedule)
                            {
                                // Generate deterministic ID
                                var runTimeStr = (spec.NextRunTime ?? now).ToString("yyyyMMddHHmm");
                                
                                var batchId = GenerateDeterministicId($"{spec.Id}_{runTimeStr}");

                                // Generate shards
                                for (int i = 0; i < spec.TotalShards; i++)
                                {
                                    var runId = GenerateDeterministicId($"{batchId}_{i}");
                                    var run = new JobRun
                                    {
                                        Id = runId,
                                        SpecId = spec.Id,
                                        BatchId = batchId,
                                        ShardIndex = i,
                                        Status = Core.Enums.JobStatus.Pending,
                                        CreatedAt = now
                                    };
                                    
                                    var inserted = await _repository.InsertJobRunAsync(run);
                                    if (inserted)
                                    {
                                        _logger.LogInformation($"Created JobRun {runId} for Spec {spec.Id} Shard {i}");
                                    }
                                }
                            }

                            // Update next run time
                            await _repository.UpdateNextRunTimeAsync(spec.Id, nextRun);
                        }
                    }
                    
                    // Attempt to renew the lock
                    await _lockProvider.RenewLockAsync(LockName, _serverIdentity.ServerId, _lockExpiry, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in CronSchedulerService");
                }

                await Task.Delay(1000, stoppingToken); // Tick every second
            }
        }

        private string GenerateDeterministicId(string input)
        {
            using var sha = SHA256.Create();
            var bytes = sha.ComputeHash(Encoding.UTF8.GetBytes(input));
            return BitConverter.ToString(bytes).Replace("-", "").ToLowerInvariant().Substring(0, 32);
        }
    }
}
