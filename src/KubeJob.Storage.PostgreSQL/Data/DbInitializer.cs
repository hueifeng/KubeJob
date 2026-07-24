using Dapper;
using KubeJob.Server.Data;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Data;

public sealed class DbInitializer : IStorageInitializer
{
    private readonly string _connectionString;

    public DbInitializer(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        _connectionString = connectionString;
    }

    public void Initialize()
    {
        using var connection = new NpgsqlConnection(_connectionString);
        connection.Open();
        connection.Execute(@"
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
                ScheduleId VARCHAR(200),
                ScheduledFor TIMESTAMPTZ,
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

            CREATE UNIQUE INDEX IF NOT EXISTS UQ_Kj2_JobRuns_ScheduleOccurrence
                ON Kj2_JobRuns (ScheduleId, ScheduledFor)
                WHERE ScheduleId IS NOT NULL AND ScheduledFor IS NOT NULL;

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_ScheduleActive
                ON Kj2_JobRuns (ScheduleId, Phase)
                WHERE ScheduleId IS NOT NULL AND Phase IN (0, 1);

            CREATE TABLE IF NOT EXISTS Kj2_JobAttempts (
                Id VARCHAR(64) PRIMARY KEY,
                RunId VARCHAR(64) NOT NULL REFERENCES Kj2_JobRuns(Id) ON DELETE CASCADE,
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

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobAttempts_WorkerActive
                ON Kj2_JobAttempts (WorkerId, SessionId, SessionEpoch)
                WHERE Phase = 0;

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
                CONSTRAINT UQ_Kj2_WorkerSessions_Epoch UNIQUE (WorkerId, Epoch),
                CONSTRAINT CK_Kj2_WorkerSessions_Capacity CHECK (MaxConcurrency >= 1),
                CONSTRAINT CK_Kj2_WorkerSessions_Slots CHECK (AvailableSlots >= 0 AND AvailableSlots <= MaxConcurrency)
            );

            CREATE INDEX IF NOT EXISTS IX_Kj2_WorkerSessions_Heartbeat
                ON Kj2_WorkerSessions (LastHeartbeatAt)
                WHERE State IN (0, 1);

            CREATE TABLE IF NOT EXISTS Kj2_JobSchedules (
                Id VARCHAR(200) PRIMARY KEY,
                JobKey VARCHAR(300) NOT NULL,
                PayloadJson JSONB NOT NULL,
                CronExpression VARCHAR(200) NOT NULL,
                TimeZoneId VARCHAR(200) NOT NULL,
                Queue VARCHAR(100) NOT NULL,
                Priority INTEGER NOT NULL DEFAULT 0,
                MisfirePolicy INTEGER NOT NULL,
                ConcurrencyPolicy INTEGER NOT NULL,
                MaxAttempts INTEGER NOT NULL,
                TimeoutSeconds INTEGER NOT NULL,
                Enabled BOOLEAN NOT NULL DEFAULT TRUE,
                NextFireAt TIMESTAMPTZ NOT NULL,
                LastFireAt TIMESTAMPTZ,
                ClaimToken VARCHAR(64),
                ClaimUntil TIMESTAMPTZ,
                CreatedAt TIMESTAMPTZ NOT NULL,
                UpdatedAt TIMESTAMPTZ NOT NULL,
                Version BIGINT NOT NULL DEFAULT 0,
                CONSTRAINT CK_Kj2_JobSchedules_MaxAttempts CHECK (MaxAttempts >= 1),
                CONSTRAINT CK_Kj2_JobSchedules_Timeout CHECK (TimeoutSeconds >= 1)
            );

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobSchedules_Due
                ON Kj2_JobSchedules (NextFireAt, Id)
                WHERE Enabled = TRUE;

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobSchedules_Claim
                ON Kj2_JobSchedules (ClaimUntil)
                WHERE ClaimToken IS NOT NULL;

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
                WHERE State IN (0, 1, 3);
        ");
    }
}
