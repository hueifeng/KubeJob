using System.Text;
using System.Text.Json;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.ControlPlane;

public sealed record CronSchedulePreview(
    string TimeZoneId,
    IReadOnlyList<DateTimeOffset> Occurrences);

/// <summary>
/// Owns cron validation, next-occurrence calculation, and schedule lifecycle
/// independently of its HTTP and typed-client adapters.
/// </summary>
public sealed class ScheduleControlPlane
{
    private readonly IJobScheduleStore _store;
    private readonly QueueCatalog _queueCatalog;
    private readonly int _maxPayloadBytes;

    public ScheduleControlPlane(
        IJobScheduleStore store,
        QueueCatalog queueCatalog,
        IOptions<JobRuntimeOptions>? options = null)
    {
        _store = store;
        _queueCatalog = queueCatalog;
        _maxPayloadBytes = options?.Value.MaxPayloadBytes
            ?? new JobRuntimeOptions().MaxPayloadBytes;
    }

    public async ValueTask<JobScheduleSnapshot> UpsertCronAsync(
        string scheduleId,
        UpsertCronScheduleRequest request,
        CancellationToken cancellationToken = default)
    {
        ValidateUpsert(scheduleId, request);

        var now = DateTimeOffset.UtcNow;
        var target = _queueCatalog.Resolve(request.Queue);
        request = NormalizeAndValidatePolicy(request, target);
        var schedule = await _store.UpsertAsync(
            CreateRecord(scheduleId, request, target, now),
            cancellationToken);

        return ToSnapshot(schedule);
    }

    public async ValueTask<JobScheduleSnapshot?> CreateCronAsync(
        string scheduleId,
        UpsertCronScheduleRequest request,
        CancellationToken cancellationToken = default)
    {
        ValidateUpsert(scheduleId, request);

        var now = DateTimeOffset.UtcNow;
        var target = _queueCatalog.Resolve(request.Queue);
        request = NormalizeAndValidatePolicy(request, target);
        var schedule = await _store.CreateIfAbsentAsync(
            CreateRecord(scheduleId, request, target, now),
            cancellationToken);
        return schedule is null ? null : ToSnapshot(schedule);
    }

    public CronSchedulePreview PreviewCron(
        string cronExpression,
        string timeZoneId,
        DateTimeOffset from,
        int count)
    {
        try
        {
            var timeZone = CronScheduleCalculator.GetTimeZone(timeZoneId);
            var occurrences = CronScheduleCalculator.GetUpcomingOccurrences(
                cronExpression,
                timeZone.Id,
                from,
                count);
            return new CronSchedulePreview(timeZone.Id, occurrences);
        }
        catch (Exception exception) when (
            CronScheduleCalculator.IsValidationException(exception))
        {
            throw new ControlPlaneValidationException(
                "invalid_schedule",
                exception.Message);
        }
    }

    public async ValueTask<JobScheduleSnapshot?> GetAsync(
        string scheduleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        var schedule = await _store.GetAsync(scheduleId, cancellationToken);
        return schedule is null ? null : ToSnapshot(schedule);
    }

    public async ValueTask<bool> SetEnabledAsync(
        string scheduleId,
        bool enabled,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        var schedule = await _store.GetAsync(scheduleId, cancellationToken);
        if (schedule is null)
        {
            return false;
        }

        DateTimeOffset? nextFireAt = null;
        if (enabled)
        {
            nextFireAt = CronScheduleCalculator.GetRequiredNextOccurrence(
                schedule.CronExpression,
                schedule.TimeZoneId,
                DateTimeOffset.UtcNow);
        }

        // Enforce the read-then-write as a single optimistic-concurrency
        // transition even when the caller didn't supply a version: without
        // this, a concurrent UpsertCronAsync landing between the read above
        // and this write would be silently clobbered by a NextFireAt computed
        // from the stale CronExpression we just read.
        return await _store.SetEnabledAsync(
            scheduleId,
            enabled,
            nextFireAt,
            expectedVersion ?? schedule.Version,
            cancellationToken: cancellationToken);
    }

    public ValueTask<bool> DeleteAsync(
        string scheduleId,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        return _store.DeleteAsync(
            scheduleId,
            expectedVersion,
            cancellationToken: cancellationToken);
    }

    private static void ValidateUpsert(
        string scheduleId,
        UpsertCronScheduleRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (string.IsNullOrWhiteSpace(scheduleId)
            || string.IsNullOrWhiteSpace(request.JobKey)
            || string.IsNullOrWhiteSpace(request.PayloadJson)
            || string.IsNullOrWhiteSpace(request.CronExpression)
            || string.IsNullOrWhiteSpace(request.TimeZoneId)
            || string.IsNullOrWhiteSpace(request.Queue)
            || request.MaxAttempts < 1
            || request.TimeoutSeconds is < 1 or > 86_400
            || !Enum.IsDefined(request.MisfirePolicy)
            || !Enum.IsDefined(request.ConcurrencyPolicy))
        {
            throw new ControlPlaneValidationException(
                "invalid_schedule",
                "Schedule id, job key, valid payload JSON, cron, time zone, queue, and positive limits are required.");
        }

        var overlongField = scheduleId.Length > 200
            ? "ScheduleId"
            : request.JobKey.Length > 300
                ? "JobKey"
                : request.Queue.Length > 100
                    ? "Queue"
                    : request.CronExpression.Length > 200
                        ? "CronExpression"
                        : request.TimeZoneId.Length > 200
                            ? "TimeZoneId"
                            : null;
        if (overlongField is not null)
        {
            throw new ControlPlaneValidationException(
                "schedule_field_too_long",
                $"{overlongField} exceeds the maximum storage length.");
        }

        try
        {
            using var payload = JsonDocument.Parse(request.PayloadJson);
            CronScheduleCalculator.Validate(request.CronExpression, request.TimeZoneId);
        }
        catch (Exception exception) when (
            exception is JsonException
                || CronScheduleCalculator.IsValidationException(exception))
        {
            throw new ControlPlaneValidationException(
                "invalid_schedule",
                exception.Message);
        }
    }

    private UpsertCronScheduleRequest NormalizeAndValidatePolicy(
        UpsertCronScheduleRequest request,
        QueueRoute route)
    {
        if (Encoding.UTF8.GetByteCount(request.PayloadJson) > _maxPayloadBytes)
        {
            throw new ControlPlaneValidationException(
                "schedule_payload_too_large",
                $"PayloadJson exceeds the configured maximum of {_maxPayloadBytes} UTF-8 bytes.");
        }

        if (request.ConcurrencyKey is { Length: > 500 })
        {
            throw new ControlPlaneValidationException(
                "schedule_field_too_long",
                "ConcurrencyKey exceeds the maximum storage length.");
        }

        if (request.RetryPolicy is { } retryPolicy)
        {
            try
            {
                retryPolicy.Validate();
            }
            catch (Exception exception) when (exception is ArgumentException or InvalidOperationException)
            {
                throw new ControlPlaneValidationException(
                    "invalid_schedule_retry_policy",
                    exception.Message);
            }
        }

        if (route.Target.OrderingMode == ExecutionOrderingMode.KeyOrdered
            && string.IsNullOrWhiteSpace(request.ConcurrencyKey))
        {
            throw new ControlPlaneValidationException(
                "ordering_key_required",
                "KeyOrdered schedules require a non-empty ConcurrencyKey as the partition key.");
        }

        var normalized = TerminalActionValidator.NormalizeAndValidate(
            request.Continuation,
            request.Compensation,
            route.Queue,
            _maxPayloadBytes,
            "invalid_schedule_terminal_action",
            "schedule_terminal_action_payload_too_large");
        return request with
        {
            Continuation = normalized.Continuation,
            Compensation = normalized.Compensation
        };
    }

    private static JobScheduleRecord CreateRecord(
        string scheduleId,
        UpsertCronScheduleRequest request,
        QueueRoute route,
        DateTimeOffset now) => new()
    {
        Id = scheduleId,
        JobKey = request.JobKey,
        PayloadJson = request.PayloadJson,
        CronExpression = request.CronExpression,
        TimeZoneId = request.TimeZoneId,
        Queue = route.Queue,
        DeliveryProfile = route.Target.Profile,
        ExecutionLane = route.Target.ExecutionLane,
        ConsumerGroup = route.Target.ConsumerGroup,
        TransportId = route.Target.TransportId,
        OrderingMode = route.Target.OrderingMode,
        Priority = request.Priority,
        MisfirePolicy = request.MisfirePolicy,
        ConcurrencyPolicy = request.ConcurrencyPolicy,
        MaxAttempts = request.MaxAttempts,
        TimeoutSeconds = request.TimeoutSeconds,
        ConcurrencyKey = request.ConcurrencyKey,
        RetryPolicy = request.RetryPolicy,
        Continuation = request.Continuation,
        Compensation = request.Compensation,
        Enabled = request.Enabled,
        NextFireAt = CronScheduleCalculator.GetRequiredNextOccurrence(
            request.CronExpression,
            request.TimeZoneId,
            now).ToUniversalTime(),
        CreatedAt = now,
        UpdatedAt = now
    };

    private static JobScheduleSnapshot ToSnapshot(JobScheduleRecord schedule) => new(
        schedule.Id,
        schedule.JobKey,
        schedule.CronExpression,
        schedule.TimeZoneId,
        schedule.Enabled,
        schedule.NextFireAt,
        schedule.LastFireAt,
        schedule.MisfirePolicy,
        schedule.ConcurrencyPolicy,
        schedule.ConcurrencyKey,
        schedule.RetryPolicy,
        schedule.Continuation,
        schedule.Compensation);
}
