namespace KubeJob.Server.Data;

public readonly record struct ScheduleMaterializationResult(
    int ProcessedSpecs,
    int InsertedRuns,
    int SkippedByConcurrency,
    int EmptyBroadcasts,
    int InvalidSchedules);

public interface IKubeJobScheduleMaterializer
{
    Task<ScheduleMaterializationResult> MaterializeDueSchedulesAsync(
        int limit,
        CancellationToken cancellationToken);
}
