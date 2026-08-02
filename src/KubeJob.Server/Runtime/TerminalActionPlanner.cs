using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Fully-resolved follow-up job to create when a parent run reaches a terminal
/// state. Stores apply this spec to their own persistence model; lineage
/// metadata (e.g. <c>_continuationOf</c>) is store-specific.
/// </summary>
public sealed record FollowUpRunSpec(
    string JobKey,
    string PayloadJson,
    string Queue,
    ExecutionDeliveryProfile DeliveryProfile,
    string ExecutionLane,
    string ConsumerGroup,
    string? TransportId,
    int Priority,
    int MaxAttempts,
    int TimeoutSeconds,
    ExecutionOrderingMode OrderingMode,
    string? ConcurrencyKey);

/// <summary>
/// Execution context a follow-up job inherits from its parent run.
/// </summary>
public sealed record FollowUpInheritance(
    string Queue,
    ExecutionDeliveryProfile DeliveryProfile,
    string ExecutionLane,
    string ConsumerGroup,
    string? TransportId,
    int Priority,
    int MaxAttempts,
    int TimeoutSeconds,
    ExecutionOrderingMode OrderingMode,
    string? ConcurrencyKey);

/// <summary>
/// Shared decision logic for per-run continuation and compensation jobs.
/// Both the in-memory and PostgreSQL stores apply identical rules when a run
/// reaches a terminal state, so behavior does not depend on the storage backend.
/// </summary>
public static class TerminalActionPlanner
{
    /// <summary>
    /// Whether a continuation configured with <paramref name="trigger"/> should
    /// fire for <paramref name="outcome"/>. <see cref="ContinuationTrigger.OnAnyTerminal"/>
    /// covers every outcome that lands in a terminal phase, including retry
    /// exhaustion (which completes with a retryable outcome but ends in Dead).
    /// </summary>
    public static bool ShouldFireContinuation(
        ContinuationTrigger trigger,
        JobAttemptOutcome outcome)
    {
        return trigger switch
        {
            ContinuationTrigger.OnSuccess => outcome == JobAttemptOutcome.Succeeded,
            ContinuationTrigger.OnPermanentFailure => outcome == JobAttemptOutcome.PermanentFailure,
            ContinuationTrigger.OnAnyTerminal => outcome is JobAttemptOutcome.Succeeded
                or JobAttemptOutcome.PermanentFailure
                or JobAttemptOutcome.RetryableFailure
                or JobAttemptOutcome.TimedOut,
            _ => false
        };
    }

    /// <summary>
    /// Whether a compensation action should fire for <paramref name="outcome"/>.
    /// </summary>
    public static bool ShouldFireCompensation(JobAttemptOutcome outcome)
    {
        return outcome is JobAttemptOutcome.PermanentFailure or JobAttemptOutcome.TimedOut;
    }

    /// <summary>
    /// Resolves the follow-up run for <paramref name="continuation"/>, or null
    /// when the trigger does not match <paramref name="outcome"/>.
    /// </summary>
    public static FollowUpRunSpec? PlanContinuation(
        Continuation continuation,
        JobAttemptOutcome outcome,
        FollowUpInheritance parent)
    {
        if (!ShouldFireContinuation(continuation.Trigger, outcome))
        {
            return null;
        }

        // Follow-ups inherit the parent's ordering contract: a continuation of
        // a KeyOrdered run continues that key's chain (same key blocks and is
        // blocked by same-key work), and a StrictFifo follow-up joins the back
        // of the lane instead of bypassing the ordering gate as Parallel.
        return new FollowUpRunSpec(
            continuation.JobKey,
            continuation.PayloadJson ?? "{}",
            continuation.Queue ?? parent.Queue,
            parent.DeliveryProfile,
            parent.ExecutionLane,
            parent.ConsumerGroup,
            parent.TransportId,
            parent.Priority,
            parent.MaxAttempts,
            parent.TimeoutSeconds,
            parent.OrderingMode,
            parent.ConcurrencyKey);
    }

    /// <summary>
    /// Resolves the follow-up run for <paramref name="compensation"/>, or null
    /// when the outcome does not warrant compensation.
    /// </summary>
    public static FollowUpRunSpec? PlanCompensation(
        Compensation compensation,
        JobAttemptOutcome outcome,
        FollowUpInheritance parent)
    {
        if (!ShouldFireCompensation(outcome))
        {
            return null;
        }

        return new FollowUpRunSpec(
            compensation.JobKey,
            compensation.PayloadJson ?? "{}",
            compensation.Queue ?? parent.Queue,
            parent.DeliveryProfile,
            parent.ExecutionLane,
            parent.ConsumerGroup,
            parent.TransportId,
            parent.Priority,
            parent.MaxAttempts,
            parent.TimeoutSeconds,
            parent.OrderingMode,
            parent.ConcurrencyKey);
    }
}
