using KubeJob.Core.Scheduling;

namespace KubeJob.Core.Runtime;

public sealed record UpsertCronScheduleRequest(
    string JobKey,
    string PayloadJson,
    string CronExpression,
    string TimeZoneId = "UTC",
    string Queue = "default",
    int Priority = 0,
    MisfirePolicy MisfirePolicy = MisfirePolicy.FireOnce,
    ScheduleConcurrencyPolicy ConcurrencyPolicy = ScheduleConcurrencyPolicy.Allow,
    int MaxAttempts = 1,
    int TimeoutSeconds = 300,
    bool Enabled = true);

public sealed record SetScheduleEnabledRequest(bool Enabled);
