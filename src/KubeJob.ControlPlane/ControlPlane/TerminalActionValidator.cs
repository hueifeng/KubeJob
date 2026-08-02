using System.Text;
using System.Text.Json;
using KubeJob.Core.Queues;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.ControlPlane;

/// <summary>
/// Validates and canonicalizes terminal actions at every control-plane entry
/// point. Keeping this rule in one module prevents scheduled runs and directly
/// submitted runs from persisting different action contracts.
/// </summary>
internal static class TerminalActionValidator
{
    public static (Continuation? Continuation, Compensation? Compensation)
        NormalizeAndValidate(
            Continuation? continuation,
            Compensation? compensation,
            string canonicalQueue,
            int maxPayloadBytes,
            string invalidActionCode = "invalid_terminal_action",
            string payloadTooLargeCode = "terminal_action_payload_too_large")
    {
        if (continuation is not null)
        {
            if (!Enum.IsDefined(continuation.Trigger))
            {
                throw new ControlPlaneValidationException(
                    invalidActionCode,
                    "Continuation.Trigger is not supported.");
            }

            continuation = continuation with
            {
                Queue = ValidateAndNormalize(
                    "continuation",
                    continuation.JobKey,
                    continuation.PayloadJson,
                    continuation.Queue,
                    canonicalQueue,
                    maxPayloadBytes,
                    invalidActionCode,
                    payloadTooLargeCode)
            };
        }

        if (compensation is not null)
        {
            compensation = compensation with
            {
                Queue = ValidateAndNormalize(
                    "compensation",
                    compensation.JobKey,
                    compensation.PayloadJson,
                    compensation.Queue,
                    canonicalQueue,
                    maxPayloadBytes,
                    invalidActionCode,
                    payloadTooLargeCode)
            };
        }

        return (continuation, compensation);
    }

    private static string? ValidateAndNormalize(
        string actionName,
        string? jobKey,
        string? payloadJson,
        string? actionQueue,
        string canonicalQueue,
        int maxPayloadBytes,
        string invalidActionCode,
        string payloadTooLargeCode)
    {
        if (string.IsNullOrWhiteSpace(jobKey) || jobKey.Length > 300)
        {
            throw new ControlPlaneValidationException(
                invalidActionCode,
                $"{actionName} JobKey must contain between 1 and 300 characters.");
        }

        string? normalizedQueue = null;
        if (actionQueue is not null)
        {
            try
            {
                normalizedQueue = LogicalQueueName.Normalize(
                    actionQueue,
                    $"{actionName}.Queue");
            }
            catch (ArgumentException exception)
            {
                throw new ControlPlaneValidationException(
                    invalidActionCode,
                    exception.Message);
            }

            if (!string.Equals(normalizedQueue, canonicalQueue, StringComparison.Ordinal))
            {
                throw new ControlPlaneValidationException(
                    "cross_queue_terminal_action_not_supported",
                    $"{actionName} Queue must match the parent Queue until a separate delivery target is resolved and persisted.");
            }
        }

        if (payloadJson is null)
        {
            return normalizedQueue;
        }

        if (maxPayloadBytes < 1 || Encoding.UTF8.GetByteCount(payloadJson) > maxPayloadBytes)
        {
            throw new ControlPlaneValidationException(
                payloadTooLargeCode,
                $"{actionName} PayloadJson exceeds the configured maximum of {maxPayloadBytes} UTF-8 bytes.");
        }

        try
        {
            using var document = JsonDocument.Parse(payloadJson);
        }
        catch (JsonException)
        {
            throw new ControlPlaneValidationException(
                invalidActionCode,
                $"{actionName} PayloadJson must contain valid JSON.");
        }

        return normalizedQueue;
    }
}
