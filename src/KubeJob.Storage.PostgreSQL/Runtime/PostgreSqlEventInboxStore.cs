using Dapper;
using KubeJob.Core.Events;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Runtime;

/// <summary>
/// PostgreSQL-backed Inbox shared by all broker adapters. A capability owns a
/// single idempotency stream regardless of how many worker replicas consume it.
/// </summary>
public sealed class PostgreSqlEventInboxStore : IEventInboxStore
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlEventInboxStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    public async ValueTask<bool> IsProcessedAsync(
        string eventId,
        string consumerName,
        CancellationToken cancellationToken = default)
    {
        ValidateKey(eventId, consumerName);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteScalarAsync<bool>(new CommandDefinition(@"
            SELECT EXISTS (
                SELECT 1
                FROM Kj2_EventInbox
                WHERE EventId = @EventId AND ConsumerName = @ConsumerName);",
            new { EventId = eventId, ConsumerName = consumerName },
            cancellationToken: cancellationToken));
    }

    public async ValueTask MarkProcessedAsync(
        string eventId,
        string consumerName,
        CancellationToken cancellationToken = default)
    {
        ValidateKey(eventId, consumerName);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(@"
            INSERT INTO Kj2_EventInbox (EventId, ConsumerName, ProcessedAt)
            VALUES (@EventId, @ConsumerName, CURRENT_TIMESTAMP)
            ON CONFLICT (EventId, ConsumerName) DO NOTHING;",
            new { EventId = eventId, ConsumerName = consumerName },
            cancellationToken: cancellationToken));
    }

    private static void ValidateKey(string eventId, string consumerName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventId);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);
    }
}
