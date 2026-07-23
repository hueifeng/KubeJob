-- KubeJob distributed runtime V2 migration (PostgreSQL 14+ recommended).
-- Apply during a maintenance window and take a backup first.
BEGIN;

CREATE SEQUENCE IF NOT EXISTS Kj_WorkerSessionEpochSeq AS BIGINT;
CREATE SEQUENCE IF NOT EXISTS Kj_LeaseTokenSeq AS BIGINT;

ALTER TABLE Kj_WorkerNodes
    ADD COLUMN IF NOT EXISTS SessionId VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS SessionEpoch BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS RuntimeVersion VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS Draining BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE Kj_JobSpecs
    ADD COLUMN IF NOT EXISTS QueueName VARCHAR(100) NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS Priority INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS RequiredHandlerVersion VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS PayloadSchemaVersion INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS ScheduleTimeZone VARCHAR(100) NOT NULL DEFAULT 'UTC',
    ADD COLUMN IF NOT EXISTS ScheduleError VARCHAR(1000) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS ScheduleErrorAt TIMESTAMPTZ;

ALTER TABLE Kj_JobRuns
    ADD COLUMN IF NOT EXISTS BatchSize INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS Attempt INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS LeaseToken BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS WorkerSessionEpoch BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS LeaseExpiresAt TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS LastLeaseRenewalAt TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS AvailableAt TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS ScheduledAt TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS CancelRequestedAt TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS QueueName VARCHAR(100) NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS Priority INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS PayloadJson JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS IdempotencyKey VARCHAR(200) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS JobType VARCHAR(200) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS TimeoutSeconds INTEGER NOT NULL DEFAULT 300,
    ADD COLUMN IF NOT EXISTS MaxRetries INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS NodeSelector JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS RequiredHandlerVersion VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS PayloadSchemaVersion INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS PinnedWorkerId VARCHAR(100),
    ADD COLUMN IF NOT EXISTS PinnedSessionEpoch BIGINT;

-- Existing timestamp-without-time-zone values were written as UTC by the current code.
-- Guard each conversion so rerunning the migration cannot reinterpret timestamptz values.
DO $$
DECLARE item RECORD;
BEGIN
    FOR item IN
        SELECT * FROM (VALUES
            ('kj_workernodes','lastheartbeat'),
            ('kj_jobspecs','nextruntime'),
            ('kj_jobspecs','createdat'),
            ('kj_jobspecs','updatedat'),
            ('kj_jobruns','createdat'),
            ('kj_jobruns','starttime'),
            ('kj_jobruns','endtime'),
            ('kj_leases','acquiredat'),
            ('kj_leases','expiresat')
        ) AS v(table_name,column_name)
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.columns c
            WHERE c.table_schema=current_schema()
              AND c.table_name=item.table_name
              AND c.column_name=item.column_name
              AND c.data_type='timestamp without time zone')
        THEN
            EXECUTE format(
                'ALTER TABLE %I ALTER COLUMN %I TYPE TIMESTAMPTZ USING %I AT TIME ZONE ''UTC''',
                item.table_name,item.column_name,item.column_name);
        END IF;
    END LOOP;
END $$;

UPDATE Kj_JobRuns r
SET JobType = s.JobType,
    TimeoutSeconds = s.TimeoutSeconds,
    MaxRetries = s.MaxRetries,
    NodeSelector = COALESCE(s.NodeSelector, '{}'::jsonb),
    QueueName = s.QueueName,
    Priority = s.Priority,
    RequiredHandlerVersion = s.RequiredHandlerVersion,
    PayloadSchemaVersion = s.PayloadSchemaVersion,
    AvailableAt = COALESCE(r.CreatedAt, NOW())
FROM Kj_JobSpecs s
WHERE r.SpecId = s.Id AND r.JobType = '';

CREATE TABLE IF NOT EXISTS Kj_WorkerCapabilities (
    WorkerId VARCHAR(100) NOT NULL,
    SessionEpoch BIGINT NOT NULL,
    JobType VARCHAR(200) NOT NULL,
    HandlerVersion VARCHAR(64) NOT NULL DEFAULT '',
    PayloadSchemaVersion INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (WorkerId, SessionEpoch, JobType)
);

CREATE TABLE IF NOT EXISTS Kj_JobRunAttempts (
    RunId VARCHAR(100) NOT NULL,
    Attempt INTEGER NOT NULL,
    LeaseToken BIGINT NOT NULL,
    WorkerId VARCHAR(100) NOT NULL,
    WorkerSessionEpoch BIGINT NOT NULL,
    Status INTEGER NOT NULL,
    AssignedAt TIMESTAMPTZ NOT NULL,
    StartedAt TIMESTAMPTZ,
    EndedAt TIMESTAMPTZ,
    ErrorSummary VARCHAR(2000) NOT NULL DEFAULT '',
    PRIMARY KEY (RunId, Attempt),
    UNIQUE (LeaseToken)
);

CREATE TABLE IF NOT EXISTS Kj_JobPayloads (
    BatchId VARCHAR(100) PRIMARY KEY,
    PayloadJson JSONB NOT NULL,
    PayloadHash BYTEA NOT NULL,
    CreatedAt TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS Kj_JobSubmissions (
    SpecId VARCHAR(100) NOT NULL,
    IdempotencyKey VARCHAR(200) NOT NULL,
    BatchId VARCHAR(100) NOT NULL,
    PayloadHash BYTEA NOT NULL,
    CreatedAt TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (SpecId, IdempotencyKey)
);

-- JobStatus compatibility mapping used by partial indexes.
CREATE OR REPLACE FUNCTION Kj_IsPendingStatus(value integer)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT value = 0 $$;
CREATE OR REPLACE FUNCTION Kj_IsLeasedStatus(value integer)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT value IN (1, 2) $$;

CREATE INDEX IF NOT EXISTS IX_Kj_JobRuns_Claim
    ON Kj_JobRuns (QueueName, Priority DESC, AvailableAt, CreatedAt, Id)
    WHERE Kj_IsPendingStatus(Status);
CREATE INDEX IF NOT EXISTS IX_Kj_JobRuns_ExpiredLease
    ON Kj_JobRuns (LeaseExpiresAt, Id)
    WHERE Kj_IsLeasedStatus(Status);
CREATE INDEX IF NOT EXISTS IX_Kj_JobRuns_Owner
    ON Kj_JobRuns (TargetNodeId, WorkerSessionEpoch, Status);
CREATE INDEX IF NOT EXISTS IX_Kj_JobRuns_Batch ON Kj_JobRuns (BatchId, ShardIndex, Id);
CREATE INDEX IF NOT EXISTS IX_Kj_JobRuns_SpecStatus ON Kj_JobRuns (SpecId, Status);
CREATE INDEX IF NOT EXISTS IX_Kj_JobRuns_Pinned
    ON Kj_JobRuns (PinnedWorkerId, PinnedSessionEpoch, Status)
    WHERE PinnedWorkerId IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS UX_Kj_JobRuns_Scheduled
    ON Kj_JobRuns (SpecId, ScheduledAt, BatchId, ShardIndex)
    WHERE ScheduledAt IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_Kj_JobSpecs_Due
    ON Kj_JobSpecs (NextRunTime, Id)
    WHERE IsDisabled = FALSE AND CronExpression IS NOT NULL AND CronExpression <> '';
CREATE INDEX IF NOT EXISTS IX_Kj_Workers_Heartbeat ON Kj_WorkerNodes (LastHeartbeat, IsOffline, Draining);
CREATE INDEX IF NOT EXISTS IX_Kj_Capabilities_Job
    ON Kj_WorkerCapabilities (JobType, HandlerVersion, PayloadSchemaVersion, WorkerId, SessionEpoch);
CREATE INDEX IF NOT EXISTS IX_Kj_Attempts_Run ON Kj_JobRunAttempts (RunId, Attempt DESC);
CREATE INDEX IF NOT EXISTS IX_Kj_Payloads_Created ON Kj_JobPayloads (CreatedAt);
CREATE INDEX IF NOT EXISTS IX_Kj_Submissions_Created ON Kj_JobSubmissions (CreatedAt);

COMMIT;
