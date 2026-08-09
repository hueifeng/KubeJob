using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Core.Transport;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.ControlPlane.Runtime;

public sealed class ScheduleReconcilerService : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly IJobScheduleStore _store;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<ScheduleReconcilerService> _logger;
    private readonly IQueueRuntimeResolver? _runtimeResolver;
    private readonly IMessageTransportRegistry? _transportRegistry;

    public ScheduleReconcilerService(
        IJobScheduleStore store,
        IOptions<JobRuntimeOptions> options,
        ILogger<ScheduleReconcilerService> logger,
        IQueueRuntimeResolver? runtimeResolver = null,
        IMessageTransportRegistry? transportRegistry = null)
    {
        _store = store;
        _options = options.Value;
        _logger = logger;
        _runtimeResolver = runtimeResolver;
        _transportRegistry = transportRegistry;
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
                await Task.Delay(ApplyJitter(_options.ScheduleFailureDelay), stoppingToken);
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
        var plan = ScheduleReconciliationPlanner.Plan(schedule, observedNow, _options.ScheduleMisfireThreshold);
        var occurrenceId = CreateOccurrenceId(schedule.Id, plan.ScheduledFor);
        var idempotencyKey = $"schedule:{schedule.Id}:{plan.ScheduledFor.UtcTicks}";

        try
        {
            var runtime = ResolveRuntime(schedule.Queue);
            if (runtime.Mode == QueueRuntimeMode.BrokerNative)
            {
                await CommitBrokerNativeFireAsync(
                    claim,
                    plan,
                    occurrenceId,
                    idempotencyKey,
                    runtime,
                    cancellationToken);
                return;
            }

            await CommitManagedFireAsync(
                claim,
                plan,
                occurrenceId,
                idempotencyKey,
                cancellationToken);
        }
        catch (IdempotencyConflictException exception)
        {
            // This can only come from the PostgresManaged atomic Run creation
            // path. BrokerNative does not create a Run in this store.
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
                    occurrenceId,
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
                DateTimeOffset.UtcNow.Add(ApplyJitter(_options.ScheduleFailureDelay)),
                cancellationToken);
        }
    }

    private async ValueTask CommitManagedFireAsync(
        ClaimedSchedule claim,
        ScheduleFirePlan plan,
        string occurrenceId,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        await _store.CommitFireAsync(
            new CommitScheduleFireCommand(
                claim.Schedule.Id,
                claim.ClaimToken,
                claim.ExpectedVersion,
                plan.ScheduledFor,
                plan.NextFireAt,
                plan.CreateRun,
                occurrenceId,
                idempotencyKey),
            cancellationToken);
    }

    private async ValueTask CommitBrokerNativeFireAsync(
        ClaimedSchedule claim,
        ScheduleFirePlan plan,
        string occurrenceId,
        string idempotencyKey,
        QueueRuntimeRoute runtime,
        CancellationToken cancellationToken)
    {
        var schedule = claim.Schedule;

        if (plan.CreateRun)
        {
            var transportId = runtime.TransportId
                ?? throw new InvalidOperationException(
                    $"BrokerNative queue '{schedule.Queue}' does not have a transport configured.");
            var transportRegistry = _transportRegistry
                ?? throw new InvalidOperationException(
                    "BrokerNative schedule dispatch requires IMessageTransportRegistry.");
            var publisher = transportRegistry.GetRequiredPublisher(transportId);

            // The occurrence id is deterministic for (ScheduleId, ScheduledFor).
            // If the process crashes after broker confirmation but before the
            // schedule cursor commit, the same occurrence is redelivered with
            // the same MessageId/IdempotencyKey. That is deliberate
            // at-least-once behavior and avoids the opposite, lossy ordering of
            // committing the cursor before publishing.
            var message = new BrokerNativeJobMessage
            {
                MessageId = occurrenceId,
                JobKey = schedule.JobKey,
                Queue = schedule.Queue,
                PayloadJson = schedule.PayloadJson,
                EnqueuedAt = DateTimeOffset.UtcNow,
                Attempt = 1,
                MaxAttempts = schedule.MaxAttempts,
                TimeoutSeconds = schedule.TimeoutSeconds,
                RetryPolicy = schedule.RetryPolicy,
                IdempotencyKey = idempotencyKey,
                Headers = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["kubejob.schedule.id"] = schedule.Id,
                    ["kubejob.schedule.scheduled-for"] = plan.ScheduledFor.ToUniversalTime().ToString("O")
                }
            };
            message.Validate();

            await publisher.PublishAsync(
                new TransportPublishRequest(
                    TransportMessageKind.Job,
                    schedule.Queue,
                    new TransportMessage(
                        occurrenceId,
                        "kubejob.broker-native.job.v1",
                        JsonSerializer.SerializeToUtf8Bytes(message, SerializerOptions),
                        message.Headers,
                        message.CorrelationId,
                        message.PartitionKey),
                    RoutingKey: schedule.Queue),
                cancellationToken);
        }

        // BrokerNative execution authority is the broker, so the schedule store
        // advances only its scheduler cursor and never creates JobRun/Attempt/
        // Outbox state for this occurrence.
        await _store.CommitFireAsync(
            new CommitScheduleFireCommand(
                schedule.Id,
                claim.ClaimToken,
                claim.ExpectedVersion,
                plan.ScheduledFor,
                plan.NextFireAt,
                false,
                occurrenceId,
                idempotencyKey),
            cancellationToken);
    }

    private QueueRuntimeRoute ResolveRuntime(string queue)
        => _runtimeResolver?.Resolve(queue)
            ?? new QueueRuntimeRoute { Mode = QueueRuntimeMode.PostgresManaged };

    public static string CreateOccurrenceId(
        string scheduleId,
        DateTimeOffset scheduledFor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        var bytes = Encoding.UTF8.GetBytes($"{scheduleId}\n{scheduledFor.ToUniversalTime():O}");
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }

    /// <summary>
    /// Scatters a retry delay within [0.5, 1.5] x delay so a burst of failures
    /// (e.g. a database blip recovering) does not re-synchronize every
    /// schedule and control-plane instance onto the same retry instant.
    /// </summary>
    internal static TimeSpan ApplyJitter(TimeSpan delay)
    {
        var factor = 0.5 + (Random.Shared.NextDouble() * 1.0);
        return TimeSpan.FromTicks((long)(delay.Ticks * factor));
    }
}
