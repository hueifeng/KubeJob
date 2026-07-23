using Cronos;

namespace KubeJob.Server.Runtime;

public static class CronScheduleCalculator
{
    public static DateTimeOffset GetRequiredNextOccurrence(
        string cronExpression,
        string timeZoneId,
        DateTimeOffset from,
        bool inclusive = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cronExpression);
        ArgumentException.ThrowIfNullOrWhiteSpace(timeZoneId);

        var expression = CronExpression.Parse(cronExpression, CronFormat.Standard);
        var timeZone = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
        return expression.GetNextOccurrence(
                   from.ToUniversalTime(),
                   timeZone,
                   inclusive)
               ?? throw new InvalidOperationException(
                   $"Cron expression '{cronExpression}' has no occurrence after {from:O}.");
    }

    public static void Validate(string cronExpression, string timeZoneId)
    {
        _ = GetRequiredNextOccurrence(
            cronExpression,
            timeZoneId,
            DateTimeOffset.UtcNow);
    }
}
