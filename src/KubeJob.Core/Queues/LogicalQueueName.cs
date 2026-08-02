namespace KubeJob.Core.Queues;

/// <summary>
/// Canonical logical queue identity shared by submission, worker capability,
/// routing policy, and transport topology.
/// </summary>
public static class LogicalQueueName
{
    public const int MaximumLength = 100;

    public static string Normalize(string value, string parameterName = "queue")
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        var normalized = value.Trim();
        if (normalized.Length == 0)
        {
            throw new ArgumentException("A logical queue name is required.", parameterName);
        }

        if (normalized.Length > MaximumLength)
        {
            throw new ArgumentException(
                $"Logical queue names cannot exceed {MaximumLength} characters.",
                parameterName);
        }

        for (var index = 0; index < normalized.Length; index++)
        {
            var character = normalized[index];
            var valid = character is >= 'a' and <= 'z'
                or >= '0' and <= '9'
                or '.'
                or '-';
            if (!valid)
            {
                throw new ArgumentException(
                    "Logical queue names may contain only lowercase letters, digits, '.' and '-'.",
                    parameterName);
            }
        }

        if (normalized[0] is '.' or '-'
            || normalized[^1] is '.' or '-'
            || normalized.Contains("..", StringComparison.Ordinal)
            || normalized.Contains("--", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Logical queue names cannot start/end with '.' or '-' or contain repeated separators.",
                parameterName);
        }

        return normalized;
    }
}
