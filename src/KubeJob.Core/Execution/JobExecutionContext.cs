using KubeJob.Core.Runtime;

namespace KubeJob.Core.Execution;

/// <summary>
/// Read-only information about the current logical run and physical attempt.
/// Business handlers receive this context but resolve dependencies through constructor injection.
/// </summary>
public sealed class JobExecutionContext
{
    public required string RunId { get; init; }

    public required string AttemptId { get; init; }

    public required int AttemptNumber { get; init; }

    /// <summary>Lease token assigned to this managed attempt, when applicable.</summary>
    public string? LeaseToken { get; init; }

    /// <summary>
    /// Monotonically increasing lease generation assigned by the control plane.
    /// It lets handlers include a durable fencing value in external side effects.
    /// Broker-native executions, which do not have a managed lease, use zero.
    /// </summary>
    public long FenceVersion { get; init; }

    public string WorkerId => Worker.WorkerId;

    public string SessionId => Worker.SessionId;

    public long SessionEpoch => Worker.SessionEpoch;

    public string? BatchId { get; init; }

    public int? ShardIndex { get; init; }

    public int? ShardCount { get; init; }

    public required WorkerExecutionInfo Worker { get; init; }

    public DateTimeOffset StartedAt { get; init; }

    /// <summary>
    /// Cancellation token for this attempt: session fence, worker drain, and
    /// the attempt timeout are linked into it. Handlers must honor this token;
    /// an attempt whose handler ignores it keeps its execution slot until the
    /// handler returns.
    /// </summary>
    public required CancellationToken CancellationToken { get; init; }

    /// <summary>
    /// The <see cref="IServiceProvider"/> scoped to this attempt.
    /// Middleware and handlers resolve their dependencies from this scope.
    /// </summary>
    public required IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// A key-value bag that middleware and handlers can use to share
    /// state across the pipeline (analogous to <c>HttpContext.Items</c>).
    /// </summary>
    public IDictionary<string, object?> Items { get; } = new Dictionary<string, object?>();

    /// <summary>
    /// When set by middleware, the worker runtime uses this outcome
    /// instead of its normal success/exception-based outcome detection.
    /// Only respected when the pipeline completes without throwing.
    /// </summary>
    public JobAttemptOutcome? Outcome { get; set; }

    /// <summary>
    /// Machine-readable failure code, used alongside <see cref="Outcome"/>.
    /// </summary>
    public string? FailureCode { get; set; }

    /// <summary>
    /// Human-readable failure message, used alongside <see cref="Outcome"/>.
    /// </summary>
    public string? FailureMessage { get; set; }
}
