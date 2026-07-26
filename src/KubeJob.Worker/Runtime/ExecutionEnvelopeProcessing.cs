using KubeJob.Core.Runtime;

namespace KubeJob.Worker.Runtime;

public enum ExecutionEnvelopeProcessingStatus
{
    Completed = 0,
    Retry = 1,
    Reject = 2
}

public sealed record ExecutionEnvelopeProcessingResult(
    ExecutionEnvelopeProcessingStatus Status,
    string? Reason = null);

public sealed record WorkerSessionContext(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    string HostName,
    string BuildId);
