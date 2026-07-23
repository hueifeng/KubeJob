using KubeJob.Server.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KubeJob.Server.Services;

public sealed partial class TransactionalCronMaterializerService : BackgroundService
{
    private readonly IKubeJobScheduleMaterializer _materializer;
    private readonly ILogger<TransactionalCronMaterializerService> _logger;

    public TransactionalCronMaterializerService(IKubeJobScheduleMaterializer materializer,
        ILogger<TransactionalCronMaterializerService> logger)
    {
        _materializer = materializer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                var result = await _materializer.MaterializeDueSchedulesAsync(256, stoppingToken);
                if (result.InsertedRuns > 0 || result.InvalidSchedules > 0)
                    Materialized(_logger, result.ProcessedSpecs, result.InsertedRuns, result.InvalidSchedules);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex) { Failed(_logger, ex); }
        }
    }

    [LoggerMessage(2101, LogLevel.Debug, "Materialized {SpecCount} specs into {RunCount} runs; {InvalidCount} invalid schedules.")]
    private static partial void Materialized(ILogger logger, int specCount, int runCount, int invalidCount);
    [LoggerMessage(2102, LogLevel.Error, "Transactional cron materialization failed.")]
    private static partial void Failed(ILogger logger, Exception exception);
}
