using System.Data;
using System.Globalization;
using System.Text.Json;
using Dapper;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Extensions;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Runtime;

public sealed partial class PostgreSqlJobRuntimeStore :
    IJobSubmissionStore,
    IWorkerSessionStore,
    IJobClaimStore,
    IJobCompletionStore,
    IJobQueryStore,
    IJobScheduleStore,
    IOutboxStore,
    IJobRuntimeDashboardStore,
    IJobRuntimeMaintenanceStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly NpgsqlDataSource _dataSource;
    private readonly SemaphoreSlim _databaseGate;

    static PostgreSqlJobRuntimeStore()
    {
        // Npgsql represents PostgreSQL timestamptz values as UTC DateTime.
        // Dapper otherwise falls back to Convert.ChangeType, which cannot
        // convert DateTime to DateTimeOffset for scalar queries.
        SqlMapper.AddTypeHandler(UtcDateTimeOffsetHandler.Instance);
    }

    public PostgreSqlJobRuntimeStore(NpgsqlDataSource dataSource)
        : this(dataSource, new PostgreSqlStorageOptions())
    {
    }

    public PostgreSqlJobRuntimeStore(
        NpgsqlDataSource dataSource,
        PostgreSqlStorageOptions options)
    {
        _dataSource = dataSource;
        _databaseGate = new SemaphoreSlim(options.MaximumConcurrentOperations);
    }

    private async ValueTask<DatabaseOperationPermit> AcquireDatabaseOperationAsync(
        CancellationToken cancellationToken)
    {
        await _databaseGate.WaitAsync(cancellationToken);
        return new DatabaseOperationPermit(_databaseGate);
    }

    private sealed class DatabaseOperationPermit : IAsyncDisposable
    {
        private readonly SemaphoreSlim _gate;
        private int _released;

        public DatabaseOperationPermit(SemaphoreSlim gate)
        {
            _gate = gate;
        }

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
            {
                _gate.Release();
            }

            return ValueTask.CompletedTask;
        }
    }

    private static string NewId() => Guid.NewGuid().ToString("N");

    private static async ValueTask<DateTimeOffset> GetDatabaseNowAsync(
        IDbConnection connection,
        IDbTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var value = await connection.ExecuteScalarAsync<DateTime>(new CommandDefinition(
            "SELECT clock_timestamp();",
            transaction: transaction,
            cancellationToken: cancellationToken));
        return UtcDateTimeOffsetHandler.FromDateTime(value);
    }

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
        string queue,
        string eventType,
        string payloadJson,
        DateTimeOffset availableAt,
        CancellationToken cancellationToken)
    {
        var command = new CommandDefinition(@"
            INSERT INTO Kj2_Outbox
                (Id, Queue, EventType, PayloadJson, State, PublishAttempts,
                 AvailableAt, CreatedAt)
            VALUES
                (@Id, @Queue, @EventType, CAST(@PayloadJson AS jsonb),
                 @State, 0, GREATEST(@AvailableAt, clock_timestamp()),
                 clock_timestamp());",
            new
            {
                Id = NewId(),
                Queue = queue,
                EventType = eventType,
                PayloadJson = payloadJson,
                State = (int)OutboxDeliveryState.Pending,
                AvailableAt = availableAt.ToUniversalTime()
            },
            transaction,
            cancellationToken: cancellationToken);
        await connection.ExecuteAsync(command);
    }

    /// <summary>
    /// Persists a per-group cancel signal as an outbox row. The cancel
    /// exchange is per-group, so the row's <c>Queue</c> column carries the
    /// group identifier (not the logical queue).
    /// </summary>
    internal static async ValueTask AddCancelOutboxAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        string group,
        string runId,
        DateTimeOffset availableAt,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        var command = new CommandDefinition(@"
            INSERT INTO Kj2_Outbox
                (Id, Queue, EventType, PayloadJson, State, PublishAttempts,
                 AvailableAt, CreatedAt)
            VALUES
                (@Id, @Queue, @EventType, CAST(@PayloadJson AS jsonb),
                 @State, 0, GREATEST(@AvailableAt, clock_timestamp()),
                 clock_timestamp());",
            new
            {
                Id = NewId(),
                Queue = group,
                EventType = OutboxEventTypes.Cancel,
                PayloadJson = JsonSerializer.Serialize(new { runId }, SerializerOptions),
                State = (int)OutboxDeliveryState.Pending,
                AvailableAt = availableAt.ToUniversalTime()
            },
            transaction,
            cancellationToken: cancellationToken);
        await connection.ExecuteAsync(command);
    }

    private sealed class UtcDateTimeOffsetHandler : SqlMapper.TypeHandler<DateTimeOffset>
    {
        public static UtcDateTimeOffsetHandler Instance { get; } = new();

        public override DateTimeOffset Parse(object value) => value switch
        {
            DateTimeOffset offset => offset.ToUniversalTime(),
            DateTime dateTime => FromDateTime(dateTime),
            string text => DateTimeOffset.Parse(
                text,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal),
            _ => throw new DataException(
                $"Cannot map database value of type '{value.GetType().FullName}' to DateTimeOffset.")
        };

        public override void SetValue(IDbDataParameter parameter, DateTimeOffset value)
        {
            parameter.Value = value.UtcDateTime;
        }

        public static DateTimeOffset FromDateTime(DateTime value)
        {
            var utc = value.Kind == DateTimeKind.Utc
                ? value
                : DateTime.SpecifyKind(value, DateTimeKind.Utc);
            return new DateTimeOffset(utc);
        }
    }
}
