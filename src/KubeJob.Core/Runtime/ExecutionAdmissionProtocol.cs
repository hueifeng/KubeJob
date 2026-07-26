namespace KubeJob.Core.Runtime;

public sealed record AdmitExecutionRequest(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    int AvailableSlots,
    string RunId,
    IReadOnlyList<string> Queues,
    IReadOnlyList<string> Capabilities);

public enum ExecutionAdmissionStatus
{
    Admitted = 0,
    Retry = 1,
    AlreadyTerminal = 2,
    NotFound = 3,
    Rejected = 4
}

public sealed record AdmitExecutionResponse(
    ExecutionAdmissionStatus Status,
    ClaimedJob? Job = null,
    string? Reason = null);
