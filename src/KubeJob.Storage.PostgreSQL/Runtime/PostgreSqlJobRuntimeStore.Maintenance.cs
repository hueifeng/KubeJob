using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<int> DeletePublishedOutboxAsync(
        DateTimeOffset olderThan,
        int batchSize,
        CancellationToken cancellationToken)
    {
        if (batchSize <= 0)
        {
            return 0;
        }

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteAsync(new CommandDefinition(@"
            WITH doomed AS (
                SELECT Id
                FROM Kj2_Outbox
                WHERE State = @Published
                  AND PublishedAt IS NOT NULL
                  AND PublishedAt <= @OlderThan
                ORDER BY PublishedAt, Id
                FOR UPDATE SKIP LOCKED
                LIMIT @BatchSize
            )
            DELETE FROM Kj2_Outbox outbox
            USING doomed
            WHERE outbox.Id = doomed.Id;",
            new
            {
                Published = (int)OutboxDeliveryState.Published,
                OlderThan = olderThan.ToUniversalTime(),
                BatchSize = batchSize
            },
            cancellationToken: cancellationToken));
    }

    public async ValueTask<int> DeleteUnkeyedTerminalRunsAsync(
        DateTimeOffset olderThan,
        int batchSize,
        CancellationToken cancellationToken)
    {
        if (batchSize <= 0)
        {
            return 0;
        }

        await using var databasePermit = await AcquireDatabaseOperationAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteAsync(new CommandDefinition(@"
            WITH doomed AS (
                SELECT Id
                FROM Kj2_JobRuns
                WHERE Phase IN (@Succeeded, @Failed, @Canceled, @Dead)
                  AND CompletedAt IS NOT NULL
                  AND CompletedAt <= @OlderThan
                  AND IdempotencyKey IS NULL
                  AND ScheduleId IS NULL
                ORDER BY CompletedAt, Id
                FOR UPDATE SKIP LOCKED
                LIMIT @BatchSize
            )
            DELETE FROM Kj2_JobRuns runs
            USING doomed
            WHERE runs.Id = doomed.Id;",
            new
            {
                Succeeded = (int)JobPhase.Succeeded,
                Failed = (int)JobPhase.Failed,
                Canceled = (int)JobPhase.Canceled,
                Dead = (int)JobPhase.Dead,
                OlderThan = olderThan.ToUniversalTime(),
                BatchSize = batchSize
            },
            cancellationToken: cancellationToken));
    }
}
