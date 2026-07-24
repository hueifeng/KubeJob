namespace KubeJob.Core.Execution;

/// <summary>
/// Read-only identity of the worker session executing the current attempt.
/// </summary>
public sealed record WorkerExecutionInfo(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    string? HostName = null,
    string? BuildId = null);
