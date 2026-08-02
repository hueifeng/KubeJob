using KubeJob.Core.Runtime;

namespace KubeJob.Worker.Runtime;

public enum ExecutionEnvelopeProcessingStatus
{
    Completed = 0,
    Retry = 1,
    Reject = 2,

    /// <summary>
    /// Admission succeeded and the execution is in flight; the durable outcome
    /// is delivered through <see cref="EnvelopeAdmissionOutcome.Completion"/>.
    /// </summary>
    Admitted = 3
}

public sealed record ExecutionEnvelopeProcessingResult(
    ExecutionEnvelopeProcessingStatus Status,
    string? Reason = null);

/// <summary>
/// Per-envelope outcome of a batch admission (same order as the input
/// envelopes). A non-null <see cref="Completion"/> means the Run was admitted
/// and is executing; the other statuses are final admission decisions.
/// </summary>
public sealed record EnvelopeAdmissionOutcome(
    ExecutionEnvelopeProcessingStatus Status,
    string? Reason,
    Task<ExecutionEnvelopeProcessingResult>? Completion);

public sealed record WorkerSessionContext(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    string HostName,
    string BuildId);
