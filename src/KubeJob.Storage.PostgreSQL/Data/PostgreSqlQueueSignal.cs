using Dapper;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Data;

internal static class PostgreSqlQueueSignal
{
    public static async Task NotifyAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT pg_notify('kubejob_runs', '')", transaction: transaction,
            cancellationToken: cancellationToken));
    }
}
