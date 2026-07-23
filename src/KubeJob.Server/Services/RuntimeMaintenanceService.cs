using KubeJob.Server.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KubeJob.Server.Services;

public sealed partial class RuntimeMaintenanceService : BackgroundService
{
    private readonly IKubeJobRuntimeRepository _repository;
    private readonly ILogger<RuntimeMaintenanceService> _logger;

    public RuntimeMaintenanceService(IKubeJobRuntimeRepository repository,
        ILogger<RuntimeMaintenanceService> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromHours(1));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                var count = await _repository.CleanupOrphanedBatchMetadataAsync(
                    TimeSpan.FromDays(7), 4096, stoppingToken);
                if (count > 0) Cleaned(_logger, count);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex) { Failed(_logger, ex); }
        }
    }

    [LoggerMessage(2301, LogLevel.Information, "Cleaned {Count} orphaned runtime metadata rows.")]
    private static partial void Cleaned(ILogger logger, int count);
    [LoggerMessage(2302, LogLevel.Error, "Runtime metadata cleanup failed.")]
    private static partial void Failed(ILogger logger, Exception exception);
}
