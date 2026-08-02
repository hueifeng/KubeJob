using KubeJob.Core.Jobs;

using KubeJob.Core.Queues;

namespace KubeJob.Core.Scheduling;

public enum MisfirePolicy
{
    /// <summary>Fire one run when one or more occurrences were missed.</summary>
    FireOnce = 0,

    /// <summary>Skip occurrences that are already behind the current scheduling window.</summary>
    SkipMissed = 1
}

public enum ScheduleConcurrencyPolicy
{
    Allow = 0,
    SkipIfRunning = 1
}

public sealed class CronScheduleOptions
{
    public string TimeZoneId { get; init; } = "UTC";
    public string? Queue { get; init; }
    public int Priority { get; init; }
    public MisfirePolicy MisfirePolicy { get; init; } = MisfirePolicy.FireOnce;
    public ScheduleConcurrencyPolicy ConcurrencyPolicy { get; init; } = ScheduleConcurrencyPolicy.Allow;
    public int MaxAttempts { get; init; } = 1;
    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(5);
    public bool Enabled { get; init; } = true;

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(TimeZoneId);
        if (Queue is not null && string.IsNullOrWhiteSpace(Queue))
        {
            throw new ArgumentException("Queue cannot be empty when explicitly specified.", nameof(Queue));
        }

        if (MaxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxAttempts));
        }

        if (Timeout <= TimeSpan.Zero || Timeout > TimeSpan.FromDays(1))
        {
            throw new ArgumentOutOfRangeException(nameof(Timeout));
        }

        if (!Enum.IsDefined(MisfirePolicy))
        {
            throw new ArgumentOutOfRangeException(nameof(MisfirePolicy));
        }

        if (!Enum.IsDefined(ConcurrencyPolicy))
        {
            throw new ArgumentOutOfRangeException(nameof(ConcurrencyPolicy));
        }
    }

    public string ResolveQueue(string jobKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobKey);
        Validate();
        return LogicalQueueName.Normalize(Queue?.Trim() ?? jobKey, nameof(Queue));
    }
}

public sealed record JobScheduleHandle(string ScheduleId);

public sealed record JobScheduleSnapshot(
    string ScheduleId,
    string JobKey,
    string CronExpression,
    string TimeZoneId,
    bool Enabled,
    DateTimeOffset NextFireAt,
    DateTimeOffset? LastFireAt,
    MisfirePolicy MisfirePolicy,
    ScheduleConcurrencyPolicy ConcurrencyPolicy);

public interface IJobScheduleClient
{
    ValueTask<JobScheduleHandle> UpsertCronAsync<TPayload>(
        string scheduleId,
        JobKey<TPayload> job,
        TPayload payload,
        string cronExpression,
        CronScheduleOptions? options = null,
        CancellationToken cancellationToken = default);

    ValueTask<JobScheduleSnapshot?> GetAsync(
        string scheduleId,
        CancellationToken cancellationToken = default);

    ValueTask<bool> SetEnabledAsync(
        string scheduleId,
        bool enabled,
        CancellationToken cancellationToken = default);

    ValueTask<bool> DeleteAsync(
        string scheduleId,
        CancellationToken cancellationToken = default);
}
