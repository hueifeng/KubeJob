using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using Dapper;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Storage.PostgreSQL.Telemetry;
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

    /// <summary>
    /// Backs request-driven store methods (submission, claim, completion,
    /// sessions, dashboard/query). Kept separate from
    /// <see cref="_backgroundDataSource"/> so a traffic spike on the
    /// business side cannot starve the always-on background loops.
    /// </summary>
    private readonly NpgsqlDataSource _businessDataSource;

    /// <summary>
    /// Backs the continuous background loops (outbox publisher, schedule
    /// reconciler, lease reaper, retention) exclusively.
    /// </summary>
    private readonly NpgsqlDataSource _backgroundDataSource;

    private readonly SemaphoreSlim _databaseGate;
    private readonly KubeJobPostgreSqlMetrics? _metrics;

    static PostgreSqlJobRuntimeStore()
    {
        // Npgsql represents PostgreSQL timestamptz values as UTC DateTime.
        // Dapper otherwise falls back to Convert.ChangeType, which cannot
        // convert DateTime to DateTimeOffset for scalar queries.
        SqlMapper.AddTypeHandler(UtcDateTimeOffsetHandler.Instance);

        // JSONB columns are named *Json while their CLR properties are not
        // (ContinuationJson -> JobRunRecord.Continuation). Dapper's default
        // name matching cannot pair them, so every read path returned null
        // for these fields. The type map below strips the suffix and the
        // handlers decode the JSON cells.
        SqlMapper.SetTypeMap(typeof(JobRunRecord), new JsonSuffixTypeMap());
        SqlMapper.AddTypeHandler(ContinuationJsonHandler.Instance);
        SqlMapper.AddTypeHandler(CompensationJsonHandler.Instance);
    }

    public PostgreSqlJobRuntimeStore(NpgsqlDataSource dataSource)
        : this(dataSource, dataSource, new PostgreSqlStorageOptions(), metrics: null)
    {
    }

    public PostgreSqlJobRuntimeStore(
        NpgsqlDataSource businessDataSource,
        NpgsqlDataSource backgroundDataSource,
        PostgreSqlStorageOptions options,
        KubeJobPostgreSqlMetrics? metrics = null)
    {
        _businessDataSource = businessDataSource;
        _backgroundDataSource = backgroundDataSource;
        _databaseGate = new SemaphoreSlim(options.MaximumConcurrentOperations);
        _metrics = metrics;
    }

    private async ValueTask<DatabaseOperationPermit> AcquireDatabaseOperationAsync(
        CancellationToken cancellationToken)
    {
        var startedAt = _metrics?.IsDatabaseGateWaitEnabled == true
            ? Stopwatch.GetTimestamp()
            : 0L;
        await _databaseGate.WaitAsync(cancellationToken);
        if (startedAt != 0)
        {
            _metrics!.DatabaseGateWaited(Stopwatch.GetElapsedTime(startedAt));
        }
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
        CancellationToken cancellationToken,
        DeliveryTarget? deliveryTarget = null,
        string? partitionKey = null)
    {
        var target = deliveryTarget
            ?? new DeliveryTarget(ExecutionDeliveryProfile.Pull, "default", null, "default");
        target.Validate();
        var command = new CommandDefinition(@"
            INSERT INTO Kj2_Outbox
                (Id, Queue, ExecutionLane, DeliveryProfile, ConsumerGroup, TransportId, OrderingMode, PartitionKey, EventType, PayloadJson, State, PublishAttempts,
                 AvailableAt, CreatedAt)
            VALUES
                (@Id, @Queue, @ExecutionLane, @DeliveryProfile, @ConsumerGroup, @TransportId, @OrderingMode, @PartitionKey, @EventType, CAST(@PayloadJson AS jsonb),
                 @State, 0, GREATEST(@AvailableAt, clock_timestamp()),
                 clock_timestamp());",
            new
            {
                Id = NewId(),
                Queue = queue,
                target.ExecutionLane,
                DeliveryProfile = (int)target.Profile,
                target.ConsumerGroup,
                target.TransportId,
                OrderingMode = (int)target.OrderingMode,
                PartitionKey = partitionKey,
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

    /// <summary>
    /// Maps <c>*Json</c> columns to their unsuffixed CLR properties
    /// (e.g. <c>ContinuationJson</c> → <see cref="JobRunRecord.Continuation"/>),
    /// which Dapper's default name matching cannot do. JSON cell decoding is
    /// handled by the per-type handlers.
    /// </summary>
    private sealed class JsonSuffixTypeMap : SqlMapper.ITypeMap
    {
        private readonly DefaultTypeMap _inner = new(typeof(JobRunRecord));

        public ConstructorInfo? FindConstructor(string[] names, Type[] types) =>
            _inner.FindConstructor(names, types);

        public ConstructorInfo? FindExplicitConstructor() =>
            _inner.FindExplicitConstructor();

        public SqlMapper.IMemberMap? GetConstructorParameter(ConstructorInfo constructor, string columnName) =>
            _inner.GetConstructorParameter(constructor, columnName);

        public SqlMapper.IMemberMap? GetMember(string columnName)
        {
            var member = _inner.GetMember(columnName);
            if (member is not null)
            {
                return member;
            }

            // PostgreSQL folds unquoted identifiers to lowercase, so the raw
            // column name is "continuationjson" rather than "ContinuationJson".
            if (columnName.EndsWith("Json", StringComparison.OrdinalIgnoreCase))
            {
                return _inner.GetMember(columnName[..^"Json".Length]);
            }

            return null;
        }
    }

    private sealed class ContinuationJsonHandler : SqlMapper.TypeHandler<Continuation>
    {
        public static ContinuationJsonHandler Instance { get; } = new();

        public override Continuation Parse(object value) =>
            JsonSerializer.Deserialize<Continuation>(
                (string)value,
                SerializerOptions)!;

        public override void SetValue(IDbDataParameter parameter, Continuation value)
        {
            parameter.Value = JsonSerializer.Serialize(value, SerializerOptions);
            parameter.DbType = DbType.String;
        }
    }

    private sealed class CompensationJsonHandler : SqlMapper.TypeHandler<Compensation>
    {
        public static CompensationJsonHandler Instance { get; } = new();

        public override Compensation Parse(object value) =>
            JsonSerializer.Deserialize<Compensation>(
                (string)value,
                SerializerOptions)!;

        public override void SetValue(IDbDataParameter parameter, Compensation value)
        {
            parameter.Value = JsonSerializer.Serialize(value, SerializerOptions);
            parameter.DbType = DbType.String;
        }
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
