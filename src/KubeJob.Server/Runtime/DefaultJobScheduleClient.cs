using System.Text.Json;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;

namespace KubeJob.Server.Runtime;

public sealed class DefaultJobScheduleClient : IJobScheduleClient
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly IJobScheduleStore _store;

    public DefaultJobScheduleClient(IJobScheduleStore store)
    {
        _store = store;
    }

    public async ValueTask<JobScheduleHandle> UpsertCronAsync<TPayload>(
        string scheduleId,
        JobKey<TPayload> job,
        TPayload payload,
        string cronExpression,
        CronScheduleOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        ArgumentException.ThrowIfNullOrWhiteSpace(cronExpression);
        if (job.IsEmpty)
        {
            throw new ArgumentException("The job key must be initialized.", nameof(job));
        }

        options ??= new CronScheduleOptions();
        options.Validate();
        var now = DateTimeOffset.UtcNow;
        var nextFireAt = CronScheduleCalculator.GetRequiredNextOccurrence(
            cronExpression,
            options.TimeZoneId,
            now);

        await _store.UpsertAsync(new JobScheduleRecord
        {
            Id = scheduleId,
            JobKey = job.Value,
            PayloadJson = JsonSerializer.Serialize(payload, SerializerOptions),
            CronExpression = cronExpression,
            TimeZoneId = options.TimeZoneId,
            Queue = options.Queue,
            Priority = options.Priority,
            MisfirePolicy = options.MisfirePolicy,
            ConcurrencyPolicy = options.ConcurrencyPolicy,
            MaxAttempts = options.MaxAttempts,
            TimeoutSeconds = checked((int)Math.Ceiling(options.Timeout.TotalSeconds)),
            Enabled = options.Enabled,
            NextFireAt = nextFireAt.ToUniversalTime(),
            CreatedAt = now,
            UpdatedAt = now
        }, cancellationToken);

        return new JobScheduleHandle(scheduleId);
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

        return await _store.SetEnabledAsync(
            scheduleId,
            enabled,
            nextFireAt,
            cancellationToken);
    }

    public ValueTask<bool> DeleteAsync(
        string scheduleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        return _store.DeleteAsync(scheduleId, cancellationToken);
    }

    internal static JobScheduleSnapshot ToSnapshot(JobScheduleRecord schedule) => new(
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
