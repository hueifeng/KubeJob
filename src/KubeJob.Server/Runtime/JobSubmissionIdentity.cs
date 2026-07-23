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
        return JsonElement.DeepEquals(
            leftDocument.RootElement,
            rightDocument.RootElement);
    }
}
