using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public static class JobSubmissionIdentity
{
    public static void EnsureCompatible(
        JobRunRecord existing,
        SubmitJobCommand command)
    {
        if (string.Equals(existing.JobKey, command.JobKey, StringComparison.Ordinal)
            && JsonEquals(existing.PayloadJson, command.PayloadJson))
        {
            return;
        }

        throw new IdempotencyConflictException(
            command.IdempotencyKey ?? string.Empty,
            existing.Id);
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
