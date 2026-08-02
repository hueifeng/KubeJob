using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.ControlPlane.Runtime;

public static class JobSubmissionIdentity
{
    public static void EnsureCompatible(
        JobRunRecord existing,
        SubmitJobCommand command)
    {
        if (AreCompatible(
                existing.JobKey,
                existing.PayloadJson,
                existing.Queue,
                existing.Priority,
                existing.ConcurrencyKey,
                existing.MaxAttempts,
                existing.TimeoutSeconds,
                existing.RetryPolicy,
                existing.Continuation,
                existing.Compensation,
                command))
        {
            return;
        }

        ThrowConflict(command.IdempotencyKey, existing.Id);
    }

    public static void EnsureCompatible(
        SubmitJobCommand existing,
        SubmitJobCommand command)
    {
        if (AreCompatible(
                existing.JobKey,
                existing.PayloadJson,
                existing.Queue,
                existing.Priority,
                existing.ConcurrencyKey,
                existing.MaxAttempts,
                existing.TimeoutSeconds,
                existing.RetryPolicy,
                existing.Continuation,
                existing.Compensation,
                command))
        {
            return;
        }

        ThrowConflict(command.IdempotencyKey, string.Empty);
    }

    private static bool AreCompatible(
        string existingJobKey,
        string existingPayloadJson,
        string existingQueue,
        int existingPriority,
        string? existingConcurrencyKey,
        int existingMaxAttempts,
        int existingTimeoutSeconds,
        RetryPolicy? existingRetryPolicy,
        Continuation? existingContinuation,
        Compensation? existingCompensation,
        SubmitJobCommand command) =>
        string.Equals(existingJobKey, command.JobKey, StringComparison.Ordinal)
        && JsonEquals(existingPayloadJson, command.PayloadJson)
        && string.Equals(existingQueue, command.Queue, StringComparison.Ordinal)
        && existingPriority == command.Priority
        && string.Equals(existingConcurrencyKey, command.ConcurrencyKey, StringComparison.Ordinal)
        && existingMaxAttempts == command.MaxAttempts
        && existingTimeoutSeconds == command.TimeoutSeconds
        && Equals(existingRetryPolicy, command.RetryPolicy)
        && ContinuationEquals(existingContinuation, command.Continuation)
        && CompensationEquals(existingCompensation, command.Compensation);

    private static void ThrowConflict(string? idempotencyKey, string existingJobId)
    {
        throw new IdempotencyConflictException(
            idempotencyKey ?? string.Empty,
            existingJobId);
    }

    private static bool ContinuationEquals(
        Continuation? left,
        Continuation? right) => left is null || right is null
        ? left is null && right is null
        : string.Equals(left.JobKey, right.JobKey, StringComparison.Ordinal)
          && left.Trigger == right.Trigger
          && string.Equals(left.Queue, right.Queue, StringComparison.Ordinal)
          && JsonEqualsOrOrdinal(left.PayloadJson ?? "{}", right.PayloadJson ?? "{}");

    private static bool CompensationEquals(
        Compensation? left,
        Compensation? right) => left is null || right is null
        ? left is null && right is null
        : string.Equals(left.JobKey, right.JobKey, StringComparison.Ordinal)
          && string.Equals(left.Queue, right.Queue, StringComparison.Ordinal)
          && JsonEqualsOrOrdinal(left.PayloadJson ?? "{}", right.PayloadJson ?? "{}");

    private static bool JsonEqualsOrOrdinal(string left, string right)
    {
        try
        {
            return JsonEquals(left, right);
        }
        catch (JsonException)
        {
            return string.Equals(left, right, StringComparison.Ordinal);
        }
    }

    private static bool JsonEquals(string left, string right)
    {
        using var leftDocument = JsonDocument.Parse(left);
        using var rightDocument = JsonDocument.Parse(right);
        return ElementsEqual(leftDocument.RootElement, rightDocument.RootElement);
    }

    private static bool ElementsEqual(JsonElement left, JsonElement right)
    {
        if (left.ValueKind != right.ValueKind)
        {
            return false;
        }

        switch (left.ValueKind)
        {
            case JsonValueKind.Object:
                var leftProperties = ToPropertyMap(left);
                var rightProperties = ToPropertyMap(right);
                if (leftProperties is null
                    || rightProperties is null
                    || leftProperties.Count != rightProperties.Count)
                {
                    return false;
                }

                foreach (var (name, value) in leftProperties)
                {
                    if (!rightProperties.TryGetValue(name, out var other)
                        || !ElementsEqual(value, other))
                    {
                        return false;
                    }
                }

                return true;

            case JsonValueKind.Array:
                var leftItems = left.EnumerateArray().ToArray();
                var rightItems = right.EnumerateArray().ToArray();
                return leftItems.Length == rightItems.Length
                       && leftItems.Zip(rightItems, ElementsEqual).All(equal => equal);

            case JsonValueKind.String:
                return string.Equals(left.GetString(), right.GetString(), StringComparison.Ordinal);

            case JsonValueKind.Number:
                return string.Equals(left.GetRawText(), right.GetRawText(), StringComparison.Ordinal);

            case JsonValueKind.True:
            case JsonValueKind.False:
            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                return true;

            default:
                return false;
        }
    }

    private static Dictionary<string, JsonElement>? ToPropertyMap(JsonElement element)
    {
        var properties = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var property in element.EnumerateObject())
        {
            if (!properties.TryAdd(property.Name, property.Value))
            {
                // Duplicate property names are ambiguous and are not treated as idempotently equal.
                return null;
            }
        }

        return properties;
    }
}
