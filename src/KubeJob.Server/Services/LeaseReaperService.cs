using KubeJob.Server.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KubeJob.Server.Services;

public sealed partial class LeaseReaperService : BackgroundService
{
    private readonly IKubeJobRuntimeRepository _repository;
    private readonly ILogger<LeaseReaperService> _logger;

    public LeaseReaperService(IKubeJobRuntimeRepository repository,
        ILogger<LeaseReaperService> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                var expired = await _repository.RequeueExpiredLeasesAsync(1024, stoppingToken);
                var orphaned = await _repository.FinalizeOrphanedPinnedRunsAsync(
                    TimeSpan.FromSeconds(30), 1024, stoppingToken);
                if (expired + orphaned > 0) Reaped(_logger, expired, orphaned);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex) { Failed(_logger, ex); }
        }
    }

    [LoggerMessage(2201, LogLevel.Information, "Reaped {ExpiredCount} expired leases and {OrphanedCount} orphaned broadcast runs.")]
    private static partial void Reaped(ILogger logger, int expiredCount, int orphanedCount);
    [LoggerMessage(2202, LogLevel.Error, "Lease reaper failed.")]
    private static partial void Failed(ILogger logger, Exception exception);
}
