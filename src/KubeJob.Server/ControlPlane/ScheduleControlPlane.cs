using System.Text.Json;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Runtime;

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

    public ScheduleControlPlane(IJobScheduleStore store)
    {
        _store = store;
    }

    public async ValueTask<JobScheduleSnapshot> UpsertCronAsync(
        string scheduleId,
        UpsertCronScheduleRequest request,
        CancellationToken cancellationToken = default)
    {
        ValidateUpsert(scheduleId, request);

        var now = DateTimeOffset.UtcNow;
        var schedule = await _store.UpsertAsync(
            CreateRecord(scheduleId, request, now),
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
        var schedule = await _store.CreateIfAbsentAsync(
            CreateRecord(scheduleId, request, now),
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

    private static JobScheduleRecord CreateRecord(
        string scheduleId,
        UpsertCronScheduleRequest request,
        DateTimeOffset now) => new()
    {
        Id = scheduleId,
        JobKey = request.JobKey,
        PayloadJson = request.PayloadJson,
        CronExpression = request.CronExpression,
        TimeZoneId = request.TimeZoneId,
        Queue = request.Queue,
        Priority = request.Priority,
        MisfirePolicy = request.MisfirePolicy,
        ConcurrencyPolicy = request.ConcurrencyPolicy,
        MaxAttempts = request.MaxAttempts,
        TimeoutSeconds = request.TimeoutSeconds,
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
        schedule.ConcurrencyPolicy);
}
