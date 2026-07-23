namespace KubeJob.Core.Domain;

/// <summary>Execution envelope returned by an atomic claim.</summary>
public sealed class JobLease
{
    public string RunId { get; init; } = string.Empty;
    public string SpecId { get; init; } = string.Empty;
    public string BatchId { get; init; } = string.Empty;
    public string JobType { get; init; } = string.Empty;
    public string PayloadJson { get; init; } = "{}";
    public int PayloadSchemaVersion { get; init; } = 1;
    public int Attempt { get; init; }
    public long LeaseToken { get; init; }
    public DateTimeOffset LeaseExpiresAt { get; init; }
    public int TimeoutSeconds { get; init; }
    public int ShardIndex { get; init; }
    public int TotalShards { get; init; } = 1;
    public DateTimeOffset? ScheduledAt { get; init; }
}
