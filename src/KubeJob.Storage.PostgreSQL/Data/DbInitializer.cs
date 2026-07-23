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

            // Legacy schema retained during the V2 migration window.
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

                CREATE TABLE IF NOT EXISTS Kj_Leases (
                    LockName VARCHAR(100) PRIMARY KEY,
                    HolderId VARCHAR(100) NOT NULL,
                    AcquiredAt TIMESTAMP NOT NULL,
                    ExpiresAt TIMESTAMP NOT NULL
                );
            ");

            // V2 logical run / physical attempt schema.
            conn.Execute(@"
                CREATE TABLE IF NOT EXISTS Kj2_JobRuns (
                    Id VARCHAR(64) PRIMARY KEY,
                    JobKey VARCHAR(300) NOT NULL,
                    PayloadJson JSONB NOT NULL,
                    Queue VARCHAR(100) NOT NULL,
                    Priority INTEGER NOT NULL DEFAULT 0,
                    Phase INTEGER NOT NULL,
                    AvailableAt TIMESTAMPTZ NOT NULL,
                    CreatedAt TIMESTAMPTZ NOT NULL,
                    StartedAt TIMESTAMPTZ,
                    CompletedAt TIMESTAMPTZ,
                    AttemptCount INTEGER NOT NULL DEFAULT 0,
                    MaxAttempts INTEGER NOT NULL,
                    TimeoutSeconds INTEGER NOT NULL,
                    IdempotencyKey VARCHAR(500),
                    ConcurrencyKey VARCHAR(500),
                    CurrentAttemptId VARCHAR(64),
                    CurrentWorkerId VARCHAR(200),
                    CurrentSessionId VARCHAR(64),
                    CancelRequested BOOLEAN NOT NULL DEFAULT FALSE,
                    FailureCode VARCHAR(200),
                    FailureMessage TEXT,
                    Version BIGINT NOT NULL DEFAULT 0,
                    CONSTRAINT CK_Kj2_JobRuns_MaxAttempts CHECK (MaxAttempts >= 1),
                    CONSTRAINT CK_Kj2_JobRuns_Timeout CHECK (TimeoutSeconds >= 1),
                    CONSTRAINT UQ_Kj2_JobRuns_Idempotency UNIQUE (IdempotencyKey)
                );

                CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_Claim
                    ON Kj2_JobRuns (Queue, Priority DESC, AvailableAt, CreatedAt)
                    WHERE Phase = 0 AND CancelRequested = FALSE;

                CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_RunningConcurrency
                    ON Kj2_JobRuns (ConcurrencyKey)
                    WHERE Phase = 1 AND ConcurrencyKey IS NOT NULL;

                CREATE TABLE IF NOT EXISTS Kj2_JobAttempts (
                    Id VARCHAR(64) PRIMARY KEY,
                    RunId VARCHAR(64) NOT NULL,
                    AttemptNumber INTEGER NOT NULL,
                    WorkerId VARCHAR(200) NOT NULL,
                    SessionId VARCHAR(64) NOT NULL,
                    SessionEpoch BIGINT NOT NULL,
                    LeaseToken VARCHAR(64) NOT NULL,
                    Phase INTEGER NOT NULL,
                    ClaimedAt TIMESTAMPTZ NOT NULL,
                    StartedAt TIMESTAMPTZ NOT NULL,
                    LeaseExpiresAt TIMESTAMPTZ NOT NULL,
                    CompletedAt TIMESTAMPTZ,
                    FailureCode VARCHAR(200),
                    FailureMessage TEXT,
                    CONSTRAINT UQ_Kj2_JobAttempts_RunAttempt UNIQUE (RunId, AttemptNumber)
                );

                CREATE INDEX IF NOT EXISTS IX_Kj2_JobAttempts_ExpiredLease
                    ON Kj2_JobAttempts (LeaseExpiresAt)
                    WHERE Phase = 0;

                CREATE INDEX IF NOT EXISTS IX_Kj2_JobAttempts_Run
                    ON Kj2_JobAttempts (RunId, AttemptNumber);

                CREATE TABLE IF NOT EXISTS Kj2_WorkerSessions (
                    WorkerId VARCHAR(200) NOT NULL,
                    SessionId VARCHAR(64) NOT NULL,
                    Epoch BIGINT NOT NULL,
                    BuildId VARCHAR(300),
                    HostName VARCHAR(300),
                    State INTEGER NOT NULL,
                    MaxConcurrency INTEGER NOT NULL,
                    AvailableSlots INTEGER NOT NULL,
                    Queues JSONB NOT NULL,
                    Capabilities JSONB NOT NULL,
                    Labels JSONB NOT NULL,
                    StartedAt TIMESTAMPTZ NOT NULL,
                    LastHeartbeatAt TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (WorkerId, SessionId),
                    CONSTRAINT UQ_Kj2_WorkerSessions_Epoch UNIQUE (WorkerId, Epoch)
                );

                CREATE INDEX IF NOT EXISTS IX_Kj2_WorkerSessions_Heartbeat
                    ON Kj2_WorkerSessions (LastHeartbeatAt)
                    WHERE State IN (0, 1);

                CREATE TABLE IF NOT EXISTS Kj2_Outbox (
                    Id VARCHAR(64) PRIMARY KEY,
                    Queue VARCHAR(100) NOT NULL,
                    EventType VARCHAR(100) NOT NULL,
                    PayloadJson JSONB NOT NULL,
                    State INTEGER NOT NULL,
                    PublishAttempts INTEGER NOT NULL DEFAULT 0,
                    AvailableAt TIMESTAMPTZ NOT NULL,
                    CreatedAt TIMESTAMPTZ NOT NULL,
                    PublishedAt TIMESTAMPTZ,
                    LastError TEXT
                );

                CREATE INDEX IF NOT EXISTS IX_Kj2_Outbox_Pending
                    ON Kj2_Outbox (AvailableAt, CreatedAt)
                    WHERE State IN (0, 3);
            ");
        }
    }
}
