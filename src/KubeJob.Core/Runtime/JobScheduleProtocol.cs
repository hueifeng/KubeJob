using System.Text.Json.Serialization;
using KubeJob.Core.Scheduling;

namespace KubeJob.Core.Runtime;

[method: JsonConstructor]
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
    bool Enabled = true,
    string? ConcurrencyKey = null,
    RetryPolicy? RetryPolicy = null)
{
    // Preserve the pre-policy constructor for already compiled client
    // adapters. New callers should use the primary constructor's policy
    // fields; the overload keeps this additive protocol change binary-safe.
    public UpsertCronScheduleRequest(
        string JobKey,
        string PayloadJson,
        string CronExpression,
        string TimeZoneId,
        string Queue,
        int Priority,
        MisfirePolicy MisfirePolicy,
        ScheduleConcurrencyPolicy ConcurrencyPolicy,
        int MaxAttempts,
        int TimeoutSeconds,
        bool Enabled)
        : this(
            JobKey,
            PayloadJson,
            CronExpression,
            TimeZoneId,
            Queue,
            Priority,
            MisfirePolicy,
            ConcurrencyPolicy,
            MaxAttempts,
            TimeoutSeconds,
            Enabled,
            null,
            null)
    {
    }
}

public sealed record SetScheduleEnabledRequest(bool Enabled);
