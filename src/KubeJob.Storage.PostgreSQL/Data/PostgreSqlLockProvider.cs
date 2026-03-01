using System;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using KubeJob.Server.Data;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Data
{
    public class PostgreSqlLockProvider : IKubeJobLockProvider
    {
        private readonly string _connectionString;

        public PostgreSqlLockProvider(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<bool> TryAcquireLockAsync(string lockName, string holderId, TimeSpan expiry, CancellationToken cancellationToken = default)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            var now = DateTime.UtcNow;
            var expiresAt = now.Add(expiry);

            var sql = @"
                INSERT INTO Kj_Leases (LockName, HolderId, AcquiredAt, ExpiresAt)
                VALUES (@LockName, @HolderId, @Now, @ExpiresAt)
                ON CONFLICT (LockName) DO UPDATE 
                SET HolderId = EXCLUDED.HolderId,
                    AcquiredAt = EXCLUDED.AcquiredAt,
                    ExpiresAt = EXCLUDED.ExpiresAt
                WHERE Kj_Leases.HolderId = EXCLUDED.HolderId OR Kj_Leases.ExpiresAt < @Now;";

            var affected = await conn.ExecuteAsync(sql, new
            {
                LockName = lockName,
                HolderId = holderId,
                Now = now,
                ExpiresAt = expiresAt
            });

            return affected > 0;
        }

        public async Task<bool> RenewLockAsync(string lockName, string holderId, TimeSpan expiry, CancellationToken cancellationToken = default)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            var now = DateTime.UtcNow;
            var expiresAt = now.Add(expiry);

            var sql = @"
                UPDATE Kj_Leases 
                SET ExpiresAt = @ExpiresAt
                WHERE LockName = @LockName AND HolderId = @HolderId AND ExpiresAt >= @Now;";

            var affected = await conn.ExecuteAsync(sql, new
            {
                LockName = lockName,
                HolderId = holderId,
                Now = now,
                ExpiresAt = expiresAt
            });

            return affected > 0;
        }

        public async Task ReleaseLockAsync(string lockName, string holderId, CancellationToken cancellationToken = default)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            var sql = @"
                DELETE FROM Kj_Leases
                WHERE LockName = @LockName AND HolderId = @HolderId;";

            await conn.ExecuteAsync(sql, new
            {
                LockName = lockName,
                HolderId = holderId
            });
        }
    }
}
