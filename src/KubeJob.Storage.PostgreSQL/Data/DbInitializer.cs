using System;
using Dapper;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Data
{
    public class DbInitializer : KubeJob.Server.Data.IStorageInitializer
    {
        private readonly string _connectionString;

        public DbInitializer(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            _connectionString = connectionString;
        }

        public void Initialize()
        {
            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            // 1. Create Kj_WorkerNodes table
            conn.Execute(@"
                CREATE TABLE IF NOT EXISTS Kj_WorkerNodes (
                    Id VARCHAR(100) PRIMARY KEY,
                    IpAddress VARCHAR(50),
                    Labels JSONB,
                    LastHeartbeat TIMESTAMP NOT NULL,
                    CurrentLoad INTEGER NOT NULL,
                    MaxCapacity INTEGER NOT NULL,
                    IsOffline BOOLEAN NOT NULL DEFAULT FALSE
                );
            ");

            // 2. Create Kj_JobSpecs table
            conn.Execute(@"
                CREATE TABLE IF NOT EXISTS Kj_JobSpecs (
                    Id VARCHAR(100) PRIMARY KEY,
                    Name VARCHAR(200) NOT NULL UNIQUE,
                    JobType VARCHAR(200) NOT NULL,
                    CronExpression VARCHAR(100),
                    NodeSelector JSONB,
                    ExecuteModel INTEGER NOT NULL,
                    TotalShards INTEGER NOT NULL,
                    NextRunTime TIMESTAMP,
                    IsDisabled BOOLEAN NOT NULL DEFAULT FALSE,
                    TimeoutSeconds INTEGER NOT NULL,
                    MaxRetries INTEGER NOT NULL,
                    ConcurrencyPolicy INTEGER NOT NULL,
                    SuccessfulJobsHistoryLimit INTEGER NOT NULL,
                    FailedJobsHistoryLimit INTEGER NOT NULL,
                    CreatedAt TIMESTAMP DEFAULT NOW(),
                    UpdatedAt TIMESTAMP DEFAULT NOW()
                );
            ");

            // 3. Create Kj_JobRuns table
            conn.Execute(@"
                CREATE TABLE IF NOT EXISTS Kj_JobRuns (
                    Id VARCHAR(100) PRIMARY KEY,
                    SpecId VARCHAR(100) NOT NULL,
                    BatchId VARCHAR(100) NOT NULL,
                    ShardIndex INTEGER NOT NULL,
                    Status INTEGER NOT NULL,
                    TargetNodeId VARCHAR(100),
                    CreatedAt TIMESTAMP NOT NULL,
                    StartTime TIMESTAMP,
                    EndTime TIMESTAMP,
                    ResultMsg TEXT,
                    RowVersion VARCHAR(50)
                );
            ");
            // 4. Create Kj_Leases table
            conn.Execute(@"
                CREATE TABLE IF NOT EXISTS Kj_Leases (
                    LockName VARCHAR(100) PRIMARY KEY,
                    HolderId VARCHAR(100) NOT NULL,
                    AcquiredAt TIMESTAMP NOT NULL,
                    ExpiresAt TIMESTAMP NOT NULL
                );
            ");
        }
    }
}
