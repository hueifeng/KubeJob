namespace KubeJob.Core.Runtime;

public sealed record AdmitExecutionRequest(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    int AvailableSlots,
    string RunId,
    IReadOnlyList<string> Queues,
    IReadOnlyList<string> Capabilities,
    string ConsumerGroup = "default",
    string ExecutionLane = "default");

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

/// <summary>
/// Admits several broker-delivered envelopes in one claim transaction. All
/// RunIds share the same worker session, queues, and capabilities; each Run is
/// still admitted individually by the durable claim gate, so KeyOrdered and
/// StrictFifo semantics are unchanged. Results preserve input order.
/// </summary>
public sealed record AdmitExecutionBatchRequest(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    int AvailableSlots,
    IReadOnlyList<string> RunIds,
    IReadOnlyList<string> Queues,
    IReadOnlyList<string> Capabilities,
    string ConsumerGroup = "default",
    string ExecutionLane = "default");

/// <summary>
/// Per-Run outcome of a batch admission, in the same order as the request's
/// <see cref="AdmitExecutionBatchRequest.RunIds"/>.
/// </summary>
public sealed record AdmitExecutionResult(
    string RunId,
    ExecutionAdmissionStatus Status,
    ClaimedJob? Job = null,
    string? Reason = null);

public sealed record AdmitExecutionBatchResponse(
    IReadOnlyList<AdmitExecutionResult> Results);
