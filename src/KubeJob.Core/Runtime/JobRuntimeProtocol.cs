using KubeJob.Core.Client;

namespace KubeJob.Core.Runtime;

public sealed record RegisterWorkerSessionRequest(
    string WorkerId,
    string SessionId,
    string? BuildId,
    string? HostName,
    int MaxConcurrency,
    IReadOnlyList<string> Queues,
    IReadOnlyList<string> Capabilities,
    IReadOnlyDictionary<string, string> Labels,
    string ConsumerGroup = "default",
    string ExecutionLane = "default");

public sealed record RegisterWorkerSessionResponse(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    DateTimeOffset RegisteredAt);

public sealed record WorkerHeartbeatRequest(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    int AvailableSlots,
    WorkerSessionState State);

/// <summary>
/// Claims eligible work for a Worker session. When <paramref name="RunIds"/>
/// is supplied, the claim is targeted and cannot fall back to another Run.
/// </summary>
public sealed record ClaimJobsRequest(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    int AvailableSlots,
    IReadOnlyList<string> Queues,
    IReadOnlyList<string> Capabilities,
    IReadOnlyList<string>? RunIds = null,
    string ConsumerGroup = "default",
    string ExecutionLane = "default");

/// <summary>
/// A claimed Run returned to a worker. The control plane populates
/// <see cref="OrderingMode"/> and <see cref="AvailableAt"/> so the managed
/// worker can record KeyOrdered wait duration without a second database read.
/// </summary>
public sealed record ClaimedJob(
    string RunId,
    string AttemptId,
    int AttemptNumber,
    string LeaseToken,
    DateTimeOffset LeaseExpiresAt,
    string JobKey,
    string PayloadJson,
    string Queue,
    int TimeoutSeconds,
    ExecutionOrderingMode OrderingMode = ExecutionOrderingMode.Parallel,
    DateTimeOffset AvailableAt = default,
    long FenceVersion = 0);

public sealed record ClaimJobsResponse(IReadOnlyList<ClaimedJob> Jobs);

public sealed record LeaseRenewal(
    string AttemptId,
    string LeaseToken,
    long FenceVersion = 0);

public sealed record RenewLeasesRequest(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    IReadOnlyList<LeaseRenewal> Attempts);

public sealed record LeaseRenewalResult(
    string AttemptId,
    bool Renewed,
    bool CancelRequested,
    DateTimeOffset? LeaseExpiresAt,
    string? RejectionReason = null);

public sealed record RenewLeasesResponse(IReadOnlyList<LeaseRenewalResult> Attempts);

public sealed record CompleteAttemptRequest(
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    string RunId,
    string AttemptId,
    int AttemptNumber,
    string LeaseToken,
    JobAttemptOutcome Outcome,
    string? FailureCode = null,
    string? FailureMessage = null,
    long FenceVersion = 0);

public sealed record CompleteAttemptResponse(
    bool Accepted,
    JobPhase Phase,
    bool Requeued,
    string? RejectionReason = null);

public sealed record RequeueExecutionRequest(
    string RunId,
    DateTimeOffset AvailableAt);

public sealed record EnqueueJobRequest(
    string JobKey,
    string PayloadJson,
    string Queue = "default",
    int Priority = 0,
    DateTimeOffset? NotBefore = null,
    string? IdempotencyKey = null,
    string? ConcurrencyKey = null,
    int MaxAttempts = 1,
    int TimeoutSeconds = 300,
    RetryPolicy? RetryPolicy = null,
    Continuation? Continuation = null,
    Compensation? Compensation = null);

public sealed record CancelJobRequest(string? Reason = null);
