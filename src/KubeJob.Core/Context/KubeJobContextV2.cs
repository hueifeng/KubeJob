using Microsoft.Extensions.Logging;

namespace KubeJob.Core.Context;

/// <summary>Immutable per-attempt context. Services are scoped to this execution.</summary>
public sealed class KubeJobContextV2
{
    public required string RunId { get; init; }
    public required string SpecId { get; init; }
    public required string BatchId { get; init; }
    public required string WorkerId { get; init; }
    public long WorkerSessionEpoch { get; init; }
    public long LeaseToken { get; init; }
    public int Attempt { get; init; }
    public int ShardIndex { get; init; }
    public int TotalShards { get; init; }
    public DateTimeOffset? ScheduledAt { get; init; }
    public DateTimeOffset Deadline { get; init; }
    public ReadOnlyMemory<byte> PayloadUtf8 { get; init; }
    public required IServiceProvider Services { get; init; }
    public required ILogger Logger { get; init; }
}
