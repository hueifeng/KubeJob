namespace KubeJob.Core.Runtime;

/// <summary>
/// Describes how a completed run triggers a follow-up run.
/// Inspired by Hangfire's <c>ContinueWith</c> and AWS Step Functions
/// <c>Next</c> transitions.
///
/// <para>
/// When a run completes with the specified <see cref="Trigger"/> outcome,
/// the control plane automatically enqueues the job identified by
/// <see cref="JobKey"/> using <see cref="PayloadJson"/> as the payload.
/// </para>
/// </summary>
public sealed record Continuation
{
    /// <summary>
    /// The job key to enqueue when this run completes.
    /// </summary>
    public required string JobKey { get; init; }

    /// <summary>
    /// The pre-serialized JSON payload to pass to the continuation job.
    /// When <c>null</c>, no payload is passed (the continuation handler
    /// receives an empty signal).
    /// </summary>
    public string? PayloadJson { get; init; }

    /// <summary>
    /// Outcome that triggers the continuation. Default: Succeeded.
    /// </summary>
    public ContinuationTrigger Trigger { get; init; } = ContinuationTrigger.OnSuccess;

    /// <summary>
    /// Optional queue for the continuation job. When <c>null</c> the
    /// original run's queue is reused.
    /// </summary>
    public string? Queue { get; init; }
}

/// <summary>
/// When to fire the continuation.
/// </summary>
public enum ContinuationTrigger
{
    /// <summary>Fire only when the run succeeds.</summary>
    OnSuccess = 0,

    /// <summary>Fire only when the run fails permanently.</summary>
    OnPermanentFailure = 1,

    /// <summary>Fire after the run reaches a terminal state (success or dead).</summary>
    OnAnyTerminal = 2,
}

/// <summary>
/// A compensating action that fires when the original run fails.
/// Equivalent to MassTransit Courier's <c>Compensate()</c>.
/// Opposite of <see cref="Continuation"/> (which fires on success).
/// </summary>
public sealed record Compensation
{
    /// <summary>
    /// The job key to enqueue for compensation.
    /// </summary>
    public required string JobKey { get; init; }

    /// <summary>
    /// Pre-serialized JSON payload with context of the failed run.
    /// </summary>
    public string? PayloadJson { get; init; }

    /// <summary>
    /// Optional queue. When <c>null</c> the original run's queue is reused.
    /// </summary>
    public string? Queue { get; init; }
}
