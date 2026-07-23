using System.Data;
using System.Text.Json;
using Dapper;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore :
    IJobSubmissionStore,
    IWorkerSessionStore,
    IJobClaimStore,
    IJobCompletionStore,
    IJobQueryStore,
    IOutboxStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlJobRuntimeStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    private static string NewId() => Guid.NewGuid().ToString("N");

    private static bool IsTerminal(JobPhase phase) => phase is
        JobPhase.Succeeded or JobPhase.Failed or JobPhase.Canceled or JobPhase.Dead;

    private static JobAttemptPhase MapAttemptPhase(JobAttemptOutcome outcome) => outcome switch
    {
        JobAttemptOutcome.Succeeded => JobAttemptPhase.Succeeded,
        JobAttemptOutcome.RetryableFailure => JobAttemptPhase.RetryableFailure,
        JobAttemptOutcome.PermanentFailure => JobAttemptPhase.PermanentFailure,
        JobAttemptOutcome.Canceled => JobAttemptPhase.Canceled,
        JobAttemptOutcome.TimedOut => JobAttemptPhase.TimedOut,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, null)
    };

    private static async ValueTask AddOutboxAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        string runId,
        string queue,
        DateTimeOffset availableAt,
        CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var command = new CommandDefinition(@"
            INSERT INTO Kj2_Outbox
                (Id, Queue, EventType, PayloadJson, State, PublishAttempts,
                 AvailableAt, CreatedAt)
            VALUES
                (@Id, @Queue, 'work-available', CAST(@PayloadJson AS jsonb),
                 @State, 0, @AvailableAt, @CreatedAt);",
            new
            {
                Id = NewId(),
                Queue = queue,
                PayloadJson = JsonSerializer.Serialize(new { runId, queue }, SerializerOptions),
                State = (int)OutboxDeliveryState.Pending,
                AvailableAt = availableAt > now ? availableAt : now,
                CreatedAt = now
            },
            transaction,
            cancellationToken: cancellationToken);
        await connection.ExecuteAsync(command);
    }

    private sealed class WorkerSessionRow
    {
        public string WorkerId { get; set; } = string.Empty;
        public string SessionId { get; set; } = string.Empty;
        public long Epoch { get; set; }
        public string? BuildId { get; set; }
        public string? HostName { get; set; }
        public WorkerSessionState State { get; set; }
        public int MaxConcurrency { get; set; }
        public int AvailableSlots { get; set; }
        public string Queues { get; set; } = "[]";
        public string Capabilities { get; set; } = "[]";
        public string Labels { get; set; } = "{}";
        public DateTimeOffset StartedAt { get; set; }
        public DateTimeOffset LastHeartbeatAt { get; set; }

        public WorkerSessionRecord ToRecord() => new()
        {
            WorkerId = WorkerId,
            SessionId = SessionId,
            Epoch = Epoch,
            BuildId = BuildId,
            HostName = HostName,
            State = State,
            MaxConcurrency = MaxConcurrency,
            AvailableSlots = AvailableSlots,
            Queues = JsonSerializer.Deserialize<string[]>(Queues, SerializerOptions) ?? Array.Empty<string>(),
            Capabilities = JsonSerializer.Deserialize<string[]>(Capabilities, SerializerOptions) ?? Array.Empty<string>(),
            Labels = JsonSerializer.Deserialize<Dictionary<string, string>>(Labels, SerializerOptions)
                     ?? new Dictionary<string, string>(),
            StartedAt = StartedAt,
            LastHeartbeatAt = LastHeartbeatAt
        };
    }
}
