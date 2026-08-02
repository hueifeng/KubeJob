using System.Text.Json;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.ControlPlane;

namespace KubeJob.Server.Runtime;

public sealed class DefaultJobScheduleClient : IJobScheduleClient
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly ScheduleControlPlane _controlPlane;

    public DefaultJobScheduleClient(ScheduleControlPlane controlPlane)
    {
        _controlPlane = controlPlane;
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
        await _controlPlane.UpsertCronAsync(
            scheduleId,
            new UpsertCronScheduleRequest(
                job.Value,
                JsonSerializer.Serialize(payload, SerializerOptions),
                cronExpression,
                options.TimeZoneId,
                options.ResolveQueue(job.Value),
                options.Priority,
                options.MisfirePolicy,
                options.ConcurrencyPolicy,
                options.MaxAttempts,
                checked((int)Math.Ceiling(options.Timeout.TotalSeconds)),
                options.Enabled),
            cancellationToken);

        return new JobScheduleHandle(scheduleId);
    }

    public async ValueTask<JobScheduleSnapshot?> GetAsync(
        string scheduleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        return await _controlPlane.GetAsync(scheduleId, cancellationToken);
    }

    public async ValueTask<bool> SetEnabledAsync(
        string scheduleId,
        bool enabled,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        return await _controlPlane.SetEnabledAsync(
            scheduleId,
            enabled,
            cancellationToken: cancellationToken);
    }

    public ValueTask<bool> DeleteAsync(
        string scheduleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        return _controlPlane.DeleteAsync(
            scheduleId,
            cancellationToken: cancellationToken);
    }
}
