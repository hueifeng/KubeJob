using Cronos;

namespace KubeJob.Server.Runtime;

public static class CronScheduleCalculator
{
    public static IReadOnlyList<DateTimeOffset> GetUpcomingOccurrences(
        string cronExpression,
        string timeZoneId,
        DateTimeOffset from,
        int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);

        var occurrences = new List<DateTimeOffset>(count);
        var cursor = from;
        for (var index = 0; index < count; index++)
        {
            cursor = GetRequiredNextOccurrence(cronExpression, timeZoneId, cursor);
            occurrences.Add(cursor);
        }

        return occurrences;
    }

    public static DateTimeOffset GetRequiredNextOccurrence(
        string cronExpression,
        string timeZoneId,
        DateTimeOffset from,
        bool inclusive = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cronExpression);
        ArgumentException.ThrowIfNullOrWhiteSpace(timeZoneId);

        var expression = CronExpression.Parse(cronExpression, CronFormat.Standard);
        var timeZone = GetTimeZone(timeZoneId);
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

    public static TimeZoneInfo GetTimeZone(string timeZoneId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(timeZoneId);
        return TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
    }

    public static bool IsValidationException(Exception exception) =>
        exception is CronFormatException
            or TimeZoneNotFoundException
            or InvalidTimeZoneException
            or InvalidOperationException
            or ArgumentException;
}
