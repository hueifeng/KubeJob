using System.Text.Json;
using Dapper;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<DashboardOverview> GetOverviewAsync(
        int recentRunCount,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        using var grid = await connection.QueryMultipleAsync(new CommandDefinition(@"
            SELECT
                COUNT(*) FILTER (WHERE Phase = @Pending)::int AS PendingRuns,
                COUNT(*) FILTER (WHERE Phase = @Running)::int AS RunningRuns,
                COUNT(*) FILTER (WHERE Phase = @Succeeded)::int AS SucceededRuns,
                COUNT(*) FILTER (WHERE Phase = @Failed)::int AS FailedRuns,
                COUNT(*) FILTER (WHERE Phase = @Canceled)::int AS CanceledRuns,
                COUNT(*) FILTER (WHERE Phase = @Dead)::int AS DeadRuns
            FROM Kj2_JobRuns;

            SELECT
                COUNT(*) FILTER (WHERE State = @Ready)::int AS ReadyWorkers,
                COUNT(*) FILTER (WHERE State = @Draining)::int AS DrainingWorkers,
                COALESCE(SUM(MaxConcurrency) FILTER (WHERE State IN (@Ready, @Draining)), 0)::int AS TotalWorkerCapacity,
                COALESCE(SUM(AvailableSlots) FILTER (WHERE State = @Ready), 0)::int AS AvailableWorkerSlots
            FROM Kj2_WorkerSessions;

            SELECT
                COUNT(*) FILTER (WHERE Enabled = TRUE)::int AS EnabledSchedules,
                COUNT(*) FILTER (WHERE Enabled = FALSE)::int AS DisabledSchedules
            FROM Kj2_JobSchedules;

            SELECT COUNT(*)::int
            FROM Kj2_Outbox
            WHERE State IN (@OutboxPending, @OutboxPublishing, @OutboxFailed);

            SELECT *
            FROM Kj2_JobRuns
            ORDER BY CreatedAt DESC, Id DESC
            LIMIT @RecentRunCount;",
            new
            {
                Pending = (int)JobPhase.Pending,
                Running = (int)JobPhase.Running,
                Succeeded = (int)JobPhase.Succeeded,
                Failed = (int)JobPhase.Failed,
                Canceled = (int)JobPhase.Canceled,
                Dead = (int)JobPhase.Dead,
                Ready = (int)WorkerSessionState.Ready,
                Draining = (int)WorkerSessionState.Draining,
                OutboxPending = (int)OutboxDeliveryState.Pending,
                OutboxPublishing = (int)OutboxDeliveryState.Publishing,
                OutboxFailed = (int)OutboxDeliveryState.Failed,
                RecentRunCount = Math.Clamp(recentRunCount, 1, 100)
            },
            cancellationToken: cancellationToken));

        var runs = await grid.ReadSingleAsync<RunCountsRow>();
        var workers = await grid.ReadSingleAsync<WorkerCountsRow>();
        var schedules = await grid.ReadSingleAsync<ScheduleCountsRow>();
        var outbox = await grid.ReadSingleAsync<int>();
        var recent = (await grid.ReadAsync<JobRunRecord>()).ToArray();

        return new DashboardOverview(
            runs.PendingRuns,
            runs.RunningRuns,
            runs.SucceededRuns,
            runs.FailedRuns,
            runs.CanceledRuns,
            runs.DeadRuns,
            workers.ReadyWorkers,
            workers.DrainingWorkers,
            workers.TotalWorkerCapacity,
            workers.AvailableWorkerSlots,
            schedules.EnabledSchedules,
            schedules.DisabledSchedules,
            outbox,
            recent);
    }

    public async ValueTask<DashboardPage<JobRunRecord>> GetRunsAsync(
        DashboardRunQuery query,
        CancellationToken cancellationToken)
    {
        var normalized = query.Normalize();
        var phase = normalized.Phase is null ? (int?)null : (int)normalized.Phase.Value;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        using var grid = await connection.QueryMultipleAsync(new CommandDefinition(@"
            SELECT *
            FROM Kj2_JobRuns
            WHERE (@Phase IS NULL OR Phase = @Phase)
              AND (@Queue IS NULL OR Queue = @Queue)
              AND (@JobKey IS NULL OR JobKey ILIKE ('%' || @JobKey || '%'))
            ORDER BY CreatedAt DESC, Id DESC
            OFFSET @Offset
            LIMIT @PageSize;

            SELECT COUNT(*)::int
            FROM Kj2_JobRuns
            WHERE (@Phase IS NULL OR Phase = @Phase)
              AND (@Queue IS NULL OR Queue = @Queue)
              AND (@JobKey IS NULL OR JobKey ILIKE ('%' || @JobKey || '%'));",
            new
            {
                Phase = phase,
                normalized.Queue,
                normalized.JobKey,
                Offset = (normalized.Page - 1) * normalized.PageSize,
                normalized.PageSize
            },
            cancellationToken: cancellationToken));

        var items = (await grid.ReadAsync<JobRunRecord>()).ToArray();
        var total = await grid.ReadSingleAsync<int>();
        return new DashboardPage<JobRunRecord>(
            items,
            total,
            normalized.Page,
            normalized.PageSize);
    }

    public async ValueTask<IReadOnlyList<WorkerSessionRecord>> GetWorkerSessionsAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<WorkerDashboardRow>(new CommandDefinition(@"
            SELECT WorkerId,
                   SessionId,
                   Epoch,
                   BuildId,
                   HostName,
                   State,
                   MaxConcurrency,
                   AvailableSlots,
                   Queues::text AS QueuesJson,
                   Capabilities::text AS CapabilitiesJson,
                   Labels::text AS LabelsJson,
                   StartedAt,
                   LastHeartbeatAt
            FROM Kj2_WorkerSessions
            ORDER BY State, LastHeartbeatAt DESC, WorkerId, Epoch DESC;",
            cancellationToken: cancellationToken));

        return rows.Select(row => new WorkerSessionRecord
        {
            WorkerId = row.WorkerId,
            SessionId = row.SessionId,
            Epoch = row.Epoch,
            BuildId = row.BuildId,
            HostName = row.HostName,
            State = row.State,
            MaxConcurrency = row.MaxConcurrency,
            AvailableSlots = row.AvailableSlots,
            Queues = JsonSerializer.Deserialize<string[]>(row.QueuesJson, SerializerOptions)
                     ?? Array.Empty<string>(),
            Capabilities = JsonSerializer.Deserialize<string[]>(row.CapabilitiesJson, SerializerOptions)
                           ?? Array.Empty<string>(),
            Labels = JsonSerializer.Deserialize<Dictionary<string, string>>(row.LabelsJson, SerializerOptions)
                     ?? new Dictionary<string, string>(),
            StartedAt = row.StartedAt,
            LastHeartbeatAt = row.LastHeartbeatAt
        }).ToArray();
    }

    public async ValueTask<IReadOnlyList<JobScheduleRecord>> GetSchedulesAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return (await connection.QueryAsync<JobScheduleRecord>(new CommandDefinition(@"
            SELECT *
            FROM Kj2_JobSchedules
            ORDER BY Enabled DESC, NextFireAt, Id;",
            cancellationToken: cancellationToken))).ToArray();
    }

    private sealed class RunCountsRow
    {
        public int PendingRuns { get; set; }
        public int RunningRuns { get; set; }
        public int SucceededRuns { get; set; }
        public int FailedRuns { get; set; }
        public int CanceledRuns { get; set; }
        public int DeadRuns { get; set; }
    }

    private sealed class WorkerCountsRow
    {
        public int ReadyWorkers { get; set; }
        public int DrainingWorkers { get; set; }
        public int TotalWorkerCapacity { get; set; }
        public int AvailableWorkerSlots { get; set; }
    }

    private sealed class ScheduleCountsRow
    {
        public int EnabledSchedules { get; set; }
        public int DisabledSchedules { get; set; }
    }

    private sealed class WorkerDashboardRow
    {
        public string WorkerId { get; set; } = string.Empty;
        public string SessionId { get; set; } = string.Empty;
        public long Epoch { get; set; }
        public string? BuildId { get; set; }
        public string? HostName { get; set; }
        public WorkerSessionState State { get; set; }
        public int MaxConcurrency { get; set; }
        public int AvailableSlots { get; set; }
        public string QueuesJson { get; set; } = "[]";
        public string CapabilitiesJson { get; set; } = "[]";
        public string LabelsJson { get; set; } = "{}";
        public DateTimeOffset StartedAt { get; set; }
        public DateTimeOffset LastHeartbeatAt { get; set; }
    }
}
