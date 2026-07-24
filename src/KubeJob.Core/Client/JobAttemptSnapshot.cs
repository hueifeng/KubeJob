using KubeJob.Core.Runtime;

namespace KubeJob.Core.Client;

/// <summary>
/// User-facing Attempt history. Internal lease and fencing credentials are
/// intentionally excluded.
/// </summary>
public sealed record JobAttemptSnapshot(
    string AttemptId,
    int AttemptNumber,
    string WorkerId,
    string SessionId,
    long SessionEpoch,
    JobAttemptPhase Phase,
    DateTimeOffset ClaimedAt,
    DateTimeOffset StartedAt,
    DateTimeOffset LeaseExpiresAt,
    DateTimeOffset? CompletedAt,
    string? FailureCode,
    string? FailureMessage);
