namespace KubeJob.Storage.PostgreSQL.Extensions;

public sealed class PostgreSqlStorageOptions
{
    /// <summary>
    /// Fixed connection headroom reserved for the background loops beyond
    /// the outbox publisher's own concurrency: schedule reconciler, lease
    /// reaper, and retention, each holding one connection at a time.
    /// </summary>
    public const int FixedBackgroundLoopConnections = 3;

    public int BusinessPoolSize { get; set; } = 32;

    public int BackgroundPoolSize { get; set; } = 8;

    public int MaximumConcurrentOperations { get; set; } = 96;

    /// <summary>
    /// Optional cap used only to warn (not fail) if the sum of both pools
    /// would exceed the PostgreSQL server's actual max_connections. Left
    /// null to skip this check, since it cannot be verified without a live
    /// round-trip at startup.
    /// </summary>
    public int? AssumedServerMaxConnections { get; set; }

    public void Validate()
    {
        if (BusinessPoolSize is < 1 or > 10_000)
        {
            throw new InvalidOperationException(
                "PostgreSQL BusinessPoolSize must be between 1 and 10000.");
        }

        if (BackgroundPoolSize is < 1 or > 10_000)
        {
            throw new InvalidOperationException(
                "PostgreSQL BackgroundPoolSize must be between 1 and 10000.");
        }

        if (MaximumConcurrentOperations is < 1 or > 10_000)
        {
            throw new InvalidOperationException(
                "PostgreSQL MaximumConcurrentOperations must be between 1 and 10000.");
        }

        if (AssumedServerMaxConnections is < 1)
        {
            throw new InvalidOperationException(
                "PostgreSQL AssumedServerMaxConnections must be positive when specified.");
        }
    }

    /// <summary>
    /// Cross-checks the background pool against the outbox publisher's
    /// configured concurrency. Called once at registration time, after both
    /// this options object and <c>JobRuntimeOptions</c> are known, so an
    /// undersized background pool fails fast at startup instead of
    /// starving the outbox/schedule/lease-reaper/retention loops silently
    /// under production load.
    /// </summary>
    public void ValidateCapacity(int outboxPublishConcurrency)
    {
        var requiredBackgroundConnections = outboxPublishConcurrency + FixedBackgroundLoopConnections;
        if (BackgroundPoolSize < requiredBackgroundConnections)
        {
            throw new InvalidOperationException(
                $"PostgreSQL BackgroundPoolSize ({BackgroundPoolSize}) is smaller than the background " +
                $"loops require ({requiredBackgroundConnections} = OutboxPublishConcurrency " +
                $"[{outboxPublishConcurrency}] + {FixedBackgroundLoopConnections} for the schedule " +
                "reconciler, lease reaper, and retention loops). Increase BackgroundPoolSize or lower " +
                "OutboxPublishConcurrency.");
        }

        if (AssumedServerMaxConnections is { } assumedMax
            && BackgroundPoolSize + BusinessPoolSize > assumedMax)
        {
            Console.Error.WriteLine(
                $"[KubeJob] Warning: PostgreSQL BackgroundPoolSize ({BackgroundPoolSize}) + " +
                $"BusinessPoolSize ({BusinessPoolSize}) = {BackgroundPoolSize + BusinessPoolSize} exceeds " +
                $"AssumedServerMaxConnections ({assumedMax}). Confirm the PostgreSQL server's " +
                "max_connections setting can accommodate both pools.");
        }
    }
}
