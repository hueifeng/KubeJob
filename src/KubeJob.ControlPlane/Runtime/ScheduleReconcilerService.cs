using System.Security.Cryptography;
using System.Text;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.ControlPlane.Runtime;

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

                if (claims.Count > 0)
                {
                    processedAny = true;
                    await ProcessClaimsAsync(claims, now, stoppingToken);
                }

                if (!processedAny)
                {
                    await Task.Delay(_options.SchedulePollInterval, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KubeJob schedule reconciliation iteration failed");
                await Task.Delay(_options.ScheduleFailureDelay, stoppingToken);
            }
        }
    }

    private async Task ProcessClaimsAsync(
        IReadOnlyList<ClaimedSchedule> claims,
        DateTimeOffset observedNow,
        CancellationToken cancellationToken)
    {
        var concurrency = Math.Clamp(_options.ScheduleReconcileConcurrency, 1, 32);
        if (concurrency == 1 || claims.Count == 1)
        {
            foreach (var claim in claims)
            {
                await ProcessClaimAsync(claim, observedNow, cancellationToken);
            }

            return;
        }

        await Parallel.ForEachAsync(
            claims,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = concurrency,
                CancellationToken = cancellationToken
            },
            (claim, token) => new ValueTask(ProcessClaimAsync(claim, observedNow, token)));
    }

    private async Task ProcessClaimAsync(
        ClaimedSchedule claim,
        DateTimeOffset observedNow,
        CancellationToken cancellationToken)
    {
        var schedule = claim.Schedule;
        var plan = ScheduleReconciliationPlanner.Plan(schedule, observedNow);
        var runId = CreateOccurrenceId(schedule.Id, plan.ScheduledFor);
        var idempotencyKey = $"schedule:{schedule.Id}:{plan.ScheduledFor.UtcTicks}";
        try
        {
            await _store.CommitFireAsync(
                new CommitScheduleFireCommand(
                    schedule.Id,
                    claim.ClaimToken,
                    claim.ExpectedVersion,
                    plan.ScheduledFor,
                    plan.NextFireAt,
                    plan.CreateRun,
                    runId,
                    idempotencyKey),
                cancellationToken);
        }
        catch (IdempotencyConflictException exception)
        {
            _logger.LogError(
                exception,
                "Schedule {ScheduleId} occurrence {ScheduledFor} collided with existing Run {ExistingRunId}; advancing the schedule without creating a duplicate",
                schedule.Id,
                plan.ScheduledFor,
                exception.ExistingJobId);
            await _store.CommitFireAsync(
                new CommitScheduleFireCommand(
                    schedule.Id,
                    claim.ClaimToken,
                    claim.ExpectedVersion,
                    plan.ScheduledFor,
                    plan.NextFireAt,
                    false,
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

    public static string CreateOccurrenceId(
        string scheduleId,
        DateTimeOffset scheduledFor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        var bytes = Encoding.UTF8.GetBytes($"{scheduleId}\n{scheduledFor.ToUniversalTime():O}");
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }
}
