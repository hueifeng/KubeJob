using System.Security.Cryptography;
using System.Text;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Runtime;

public sealed class ScheduleReconcilerService : BackgroundService
{
    private readonly IJobScheduleStore _store;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<ScheduleReconcilerService> _logger;

    public ScheduleReconcilerService(
        IJobScheduleStore store,
        IOptions<JobRuntimeOptions> options,
        ILogger<ScheduleReconcilerService> logger)
    {
        _store = store;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _options.Validate();

        while (!stoppingToken.IsCancellationRequested)
        {
            var processedAny = false;
            try
            {
                var now = DateTimeOffset.UtcNow;
                var claims = await _store.ClaimDueAsync(
                    now,
                    _options.ScheduleClaimDuration,
                    _options.ScheduleBatchSize,
                    stoppingToken);

                foreach (var claim in claims)
                {
                    processedAny = true;
                    await ProcessClaimAsync(claim, now, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KubeJob schedule reconciliation iteration failed");
            }

            if (!processedAny)
            {
                await Task.Delay(_options.SchedulePollInterval, stoppingToken);
            }
        }
    }

    private async Task ProcessClaimAsync(
        ClaimedSchedule claim,
        DateTimeOffset observedNow,
        CancellationToken cancellationToken)
    {
        var schedule = claim.Schedule;
        try
        {
            var scheduledFor = schedule.NextFireAt.ToUniversalTime();
            var nextAfterScheduled = CronScheduleCalculator.GetRequiredNextOccurrence(
                schedule.CronExpression,
                schedule.TimeZoneId,
                scheduledFor);

            var createRun = true;
            DateTimeOffset nextFireAt;
            if (nextAfterScheduled <= observedNow)
            {
                createRun = schedule.MisfirePolicy == MisfirePolicy.FireOnce;
                nextFireAt = CronScheduleCalculator.GetRequiredNextOccurrence(
                    schedule.CronExpression,
                    schedule.TimeZoneId,
                    observedNow);
            }
            else
            {
                nextFireAt = nextAfterScheduled;
            }

            var runId = CreateOccurrenceId(schedule.Id, scheduledFor);
            var idempotencyKey = $"schedule:{schedule.Id}:{scheduledFor.UtcTicks}";
            await _store.CommitFireAsync(
                new CommitScheduleFireCommand(
                    schedule.Id,
                    claim.ClaimToken,
                    claim.ExpectedVersion,
                    scheduledFor,
                    nextFireAt,
                    createRun,
                    runId,
                    idempotencyKey),
                cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to reconcile schedule {ScheduleId} claimed with version {Version}",
                schedule.Id,
                claim.ExpectedVersion);
            await _store.ReleaseClaimAsync(
                schedule.Id,
                claim.ClaimToken,
                DateTimeOffset.UtcNow.Add(_options.ScheduleFailureDelay),
                cancellationToken);
        }
    }

    internal static string CreateOccurrenceId(
        string scheduleId,
        DateTimeOffset scheduledFor)
    {
        var bytes = Encoding.UTF8.GetBytes($"{scheduleId}\n{scheduledFor.ToUniversalTime():O}");
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }
}
