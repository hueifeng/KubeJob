using System.Text.Json;
using Dapper;
using KubeJob.Core.Runtime;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore
{
    public async ValueTask<WorkerSessionRecord> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT pg_advisory_xact_lock(hashtext(@WorkerId));",
            new { request.WorkerId },
            transaction,
            cancellationToken: cancellationToken));

        var existing = await connection.QuerySingleOrDefaultAsync<ExistingSessionRow>(new CommandDefinition(@"
            SELECT Epoch, State, StartedAt
            FROM Kj2_WorkerSessions
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
            LIMIT 1;",
            new { request.WorkerId, request.SessionId },
            transaction,
            cancellationToken: cancellationToken));

        var now = await connection.ExecuteScalarAsync<DateTimeOffset>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));

        if (existing is not null)
        {
            if (existing.State is WorkerSessionState.Closed or WorkerSessionState.Stale)
            {
                throw new InvalidOperationException("A closed or stale worker session cannot be reopened.");
            }

            await connection.ExecuteAsync(new CommandDefinition(@"
                UPDATE Kj2_WorkerSessions
                SET BuildId = @BuildId,
                    HostName = @HostName,
                    State = @Ready,
                    MaxConcurrency = @MaxConcurrency,
                    AvailableSlots = LEAST(AvailableSlots, @MaxConcurrency),
                    Queues = CAST(@Queues AS jsonb),
                    Capabilities = CAST(@Capabilities AS jsonb),
                    Labels = CAST(@Labels AS jsonb),
                    LastHeartbeatAt = @Now
                WHERE WorkerId = @WorkerId
                  AND SessionId = @SessionId
                  AND Epoch = @Epoch;",
                new
                {
                    request.WorkerId,
                    request.SessionId,
                    existing.Epoch,
                    request.BuildId,
                    request.HostName,
                    Ready = (int)WorkerSessionState.Ready,
                    request.MaxConcurrency,
                    Queues = JsonSerializer.Serialize(request.Queues, SerializerOptions),
                    Capabilities = JsonSerializer.Serialize(request.Capabilities, SerializerOptions),
                    Labels = JsonSerializer.Serialize(request.Labels, SerializerOptions),
                    Now = now
                },
                transaction,
                cancellationToken: cancellationToken));

            await transaction.CommitAsync(cancellationToken);
            return CreateSessionRecord(request, existing.Epoch, existing.StartedAt, now);
        }

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET State = @Stale,
                AvailableSlots = 0
            WHERE WorkerId = @WorkerId
              AND State IN (@Ready, @Draining);",
            new
            {
                request.WorkerId,
                Stale = (int)WorkerSessionState.Stale,
                Ready = (int)WorkerSessionState.Ready,
                Draining = (int)WorkerSessionState.Draining
            },
            transaction,
            cancellationToken: cancellationToken));

        var epoch = await connection.ExecuteScalarAsync<long>(new CommandDefinition(@"
            SELECT COALESCE(MAX(Epoch), 0) + 1
            FROM Kj2_WorkerSessions
            WHERE WorkerId = @WorkerId;",
            new { request.WorkerId },
            transaction,
            cancellationToken: cancellationToken));

        await connection.ExecuteAsync(new CommandDefinition(@"
            INSERT INTO Kj2_WorkerSessions
                (WorkerId, SessionId, Epoch, BuildId, HostName, State,
                 MaxConcurrency, AvailableSlots, Queues, Capabilities, Labels,
                 StartedAt, LastHeartbeatAt)
            VALUES
                (@WorkerId, @SessionId, @Epoch, @BuildId, @HostName, @Ready,
                 @MaxConcurrency, @MaxConcurrency, CAST(@Queues AS jsonb),
                 CAST(@Capabilities AS jsonb), CAST(@Labels AS jsonb),
                 @Now, @Now);",
            new
            {
                request.WorkerId,
                request.SessionId,
                Epoch = epoch,
                request.BuildId,
                request.HostName,
                Ready = (int)WorkerSessionState.Ready,
                request.MaxConcurrency,
                Queues = JsonSerializer.Serialize(request.Queues, SerializerOptions),
                Capabilities = JsonSerializer.Serialize(request.Capabilities, SerializerOptions),
                Labels = JsonSerializer.Serialize(request.Labels, SerializerOptions),
                Now = now
            },
            transaction,
            cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return CreateSessionRecord(request, epoch, now, now);
    }

    public async ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var session = await connection.QuerySingleOrDefaultAsync<SessionCapacityRow>(new CommandDefinition(@"
            SELECT MaxConcurrency, State
            FROM Kj2_WorkerSessions
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch
            FOR UPDATE;",
            new { request.WorkerId, request.SessionId, request.SessionEpoch },
            transaction,
            cancellationToken: cancellationToken));

        if (session is null || session.State is WorkerSessionState.Closed or WorkerSessionState.Stale)
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        var activeAttempts = await connection.ExecuteScalarAsync<int>(new CommandDefinition(@"
            SELECT COUNT(*)
            FROM Kj2_JobAttempts
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND SessionEpoch = @SessionEpoch
              AND Phase = @Running;",
            new
            {
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                Running = (int)JobAttemptPhase.Running
            },
            transaction,
            cancellationToken: cancellationToken));
        var serverAvailable = Math.Max(0, session.MaxConcurrency - activeAttempts);
        var availableSlots = Math.Min(Math.Max(request.AvailableSlots, 0), serverAvailable);

        await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET AvailableSlots = @AvailableSlots,
                State = @State,
                LastHeartbeatAt = clock_timestamp()
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch;",
            new
            {
                AvailableSlots = availableSlots,
                State = (int)request.State,
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch
            },
            transaction,
            cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async ValueTask<bool> CloseAsync(
        string workerId,
        string sessionId,
        long sessionEpoch,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var affected = await connection.ExecuteAsync(new CommandDefinition(@"
            UPDATE Kj2_WorkerSessions
            SET State = @Closed,
                AvailableSlots = 0,
                LastHeartbeatAt = clock_timestamp()
            WHERE WorkerId = @WorkerId
              AND SessionId = @SessionId
              AND Epoch = @SessionEpoch
              AND State IN (@Ready, @Draining);",
            new
            {
                WorkerId = workerId,
                SessionId = sessionId,
                SessionEpoch = sessionEpoch,
                Closed = (int)WorkerSessionState.Closed,
                Ready = (int)WorkerSessionState.Ready,
                Draining = (int)WorkerSessionState.Draining
            },
            cancellationToken: cancellationToken));
        return affected > 0;
    }

    private static WorkerSessionRecord CreateSessionRecord(
        RegisterWorkerSessionRequest request,
        long epoch,
        DateTimeOffset startedAt,
        DateTimeOffset heartbeatAt) => new()
    {
        WorkerId = request.WorkerId,
        SessionId = request.SessionId,
        Epoch = epoch,
        BuildId = request.BuildId,
        HostName = request.HostName,
        State = WorkerSessionState.Ready,
        MaxConcurrency = request.MaxConcurrency,
        AvailableSlots = request.MaxConcurrency,
        Queues = request.Queues.ToArray(),
        Capabilities = request.Capabilities.ToArray(),
        Labels = new Dictionary<string, string>(request.Labels, StringComparer.Ordinal),
        StartedAt = startedAt,
        LastHeartbeatAt = heartbeatAt
    };

    private sealed class ExistingSessionRow
    {
        public long Epoch { get; set; }
        public WorkerSessionState State { get; set; }
        public DateTimeOffset StartedAt { get; set; }
    }

    private sealed class SessionCapacityRow
    {
        public int MaxConcurrency { get; set; }
        public WorkerSessionState State { get; set; }
    }
}
