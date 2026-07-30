using Dapper;
using KubeJob.Server.Data;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Data;

public sealed class DbInitializer : IStorageInitializer
{
    private const int CurrentSchemaVersion = 2;

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
            SELECT pg_advisory_lock(hashtextextended('kubejob.schema', 0));
            CREATE TABLE IF NOT EXISTS Kj2_SchemaMigrations (
                Version INTEGER PRIMARY KEY,
                AppliedAt TIMESTAMPTZ NOT NULL
            );");

        try
        {
            var appliedVersion = connection.ExecuteScalar<int?>(
                "SELECT MAX(Version) FROM Kj2_SchemaMigrations;");
            if (appliedVersion is null)
            {
                connection.Execute(@"
            CREATE TABLE IF NOT EXISTS Kj2_JobRuns (
                Id VARCHAR(64) PRIMARY KEY,
                JobKey VARCHAR(300) NOT NULL,
                PayloadJson JSONB NOT NULL,
                Queue VARCHAR(100) NOT NULL,
                DeliveryProfile INTEGER NOT NULL DEFAULT 0,
                ExecutionLane VARCHAR(200) NOT NULL DEFAULT 'default',
                TransportId VARCHAR(100),
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

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_DashboardRecent
                ON Kj2_JobRuns (CreatedAt DESC, Id DESC);

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_DashboardPhase
                ON Kj2_JobRuns (Phase, CreatedAt DESC, Id DESC);

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_DashboardQueue
                ON Kj2_JobRuns (Queue, CreatedAt DESC, Id DESC);

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_DashboardActiveQueue
                ON Kj2_JobRuns (Queue, Phase, AvailableAt)
                WHERE Phase IN (0, 1);

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_DashboardCompleted
                ON Kj2_JobRuns (CompletedAt DESC)
                WHERE CompletedAt IS NOT NULL;

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_DashboardJobKey
                ON Kj2_JobRuns ((LOWER(JobKey)) text_pattern_ops);

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

            DROP INDEX IF EXISTS IX_Kj2_JobAttempts_Run;

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

            CREATE INDEX IF NOT EXISTS IX_Kj2_WorkerSessions_Dashboard
                ON Kj2_WorkerSessions (State, LastHeartbeatAt DESC, WorkerId, Epoch DESC);

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

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobSchedules_Dashboard
                ON Kj2_JobSchedules (Enabled DESC, NextFireAt, Id);

            CREATE TABLE IF NOT EXISTS Kj2_Outbox (
                Id VARCHAR(64) PRIMARY KEY,
                Queue VARCHAR(100) NOT NULL,
                DeliveryProfile INTEGER NOT NULL DEFAULT 0,
                ExecutionLane VARCHAR(200) NOT NULL DEFAULT 'default',
                TransportId VARCHAR(100),
                EventType VARCHAR(100) NOT NULL,
                PayloadJson JSONB NOT NULL,
                State INTEGER NOT NULL,
                PublishAttempts INTEGER NOT NULL DEFAULT 0,
                AvailableAt TIMESTAMPTZ NOT NULL,
                CreatedAt TIMESTAMPTZ NOT NULL,
                PublishedAt TIMESTAMPTZ,
                ClaimToken VARCHAR(64),
                LastError TEXT
            );

            ALTER TABLE Kj2_Outbox
                ADD COLUMN IF NOT EXISTS ClaimToken VARCHAR(64);

            CREATE INDEX IF NOT EXISTS IX_Kj2_Outbox_Pending
                ON Kj2_Outbox (AvailableAt, CreatedAt)
                WHERE State IN (0, 1, 3);

            CREATE INDEX IF NOT EXISTS IX_Kj2_Outbox_PublishedRetention
                ON Kj2_Outbox (PublishedAt, Id)
                WHERE State = 2 AND PublishedAt IS NOT NULL;

            CREATE INDEX IF NOT EXISTS IX_Kj2_JobRuns_TerminalRetention
                ON Kj2_JobRuns (CompletedAt, Id)
                WHERE Phase IN (2, 3, 4, 5)
                  AND IdempotencyKey IS NULL
                  AND IdempotencyKey IS NULL
                  AND ScheduleId IS NULL;
        ");
                connection.Execute(
                    "INSERT INTO Kj2_SchemaMigrations (Version, AppliedAt) VALUES (@Version, CURRENT_TIMESTAMP);",
                    new { Version = CurrentSchemaVersion });
            }
            else if (appliedVersion == 1)
            {
                connection.Execute(@"
                    ALTER TABLE Kj2_JobRuns
                        ADD COLUMN IF NOT EXISTS DeliveryProfile INTEGER NOT NULL DEFAULT 0,
                        ADD COLUMN IF NOT EXISTS ExecutionLane VARCHAR(200) NOT NULL DEFAULT 'default',
                        ADD COLUMN IF NOT EXISTS TransportId VARCHAR(100);
                    ALTER TABLE Kj2_Outbox
                        ADD COLUMN IF NOT EXISTS DeliveryProfile INTEGER NOT NULL DEFAULT 0,
                        ADD COLUMN IF NOT EXISTS ExecutionLane VARCHAR(200) NOT NULL DEFAULT 'default',
                        ADD COLUMN IF NOT EXISTS TransportId VARCHAR(100);");
                connection.Execute(
                    "INSERT INTO Kj2_SchemaMigrations (Version, AppliedAt) VALUES (2, CURRENT_TIMESTAMP);");
            }
            else if (appliedVersion != CurrentSchemaVersion)
            {
                throw new InvalidOperationException(
                    $"Unsupported KubeJob schema version {appliedVersion}; application supports {CurrentSchemaVersion}.");
            }

            ValidateSchemaContract(connection);
        }
        finally
        {
            connection.Execute(
                "SELECT pg_advisory_unlock(hashtextextended('kubejob.schema', 0));");
        }
    }

    private static void ValidateSchemaContract(NpgsqlConnection connection)
    {
        var missing = connection.ExecuteScalar<string?>(@"
            SELECT string_agg(expected.table_name || '.' || expected.column_name, ', ' ORDER BY expected.table_name, expected.column_name)
            FROM (VALUES
                ('Kj2_JobRuns', 'Id'),
                ('Kj2_JobRuns', 'PayloadJson'),
                ('Kj2_JobRuns', 'CancelRequested'),
                ('Kj2_JobRuns', 'Version'),
                ('Kj2_JobAttempts', 'RunId'),
                ('Kj2_JobAttempts', 'LeaseExpiresAt'),
                ('Kj2_WorkerSessions', 'Epoch'),
                ('Kj2_WorkerSessions', 'AvailableSlots'),
                ('Kj2_JobSchedules', 'ClaimToken'),
                ('Kj2_Outbox', 'EventType'),
                ('Kj2_Outbox', 'ClaimToken')) AS expected(table_name, column_name)
            LEFT JOIN information_schema.columns columns
                ON columns.table_schema = current_schema()
               AND lower(columns.table_name) = lower(expected.table_name)
               AND lower(columns.column_name) = lower(expected.column_name)
            WHERE columns.column_name IS NULL;");

        if (!string.IsNullOrWhiteSpace(missing))
        {
            throw new InvalidOperationException(
                $"KubeJob schema version {CurrentSchemaVersion} is missing required columns: {missing}.");
        }
    }
}
