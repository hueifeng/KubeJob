using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Storage.PostgreSQL.Data;
using KubeJob.Storage.PostgreSQL.Runtime;
using Npgsql;

namespace KubeJob.Tests.Runtime;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class PostgreSqlRuntimeCollection
{
    public const string Name = "postgres-runtime";
}

/// <summary>
/// PostgreSQL integration coverage for the V3 PostgresManaged runtime.
/// RabbitMQ/Kafka delivery is deliberately absent: PostgreSQL owns durable
/// Run/Attempt/Lease state here, while BrokerNative is covered by transport
/// integration tests.
/// </summary>
[Collection(PostgreSqlRuntimeCollection.Name)]
public sealed class PostgreSqlRuntimeIntegrationTests : IAsyncLifetime
{
    private static readonly RetryPolicy TestRetryPolicy =
        new(BackoffStrategy.Fixed, TimeSpan.Zero, TimeSpan.Zero);

    private string? _adminConnectionString;
    private string? _databaseName;
    private string? _testConnectionString;
    private NpgsqlDataSource? _dataSource;
    private PostgreSqlJobRuntimeStore? _store;

    private bool Enabled => _store is not null;

    public async Task InitializeAsync()
    {
        var configured = Environment.GetEnvironmentVariable("KUBEJOB_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(configured))
        {
            if (string.Equals(
                    Environment.GetEnvironmentVariable("KUBEJOB_REQUIRE_POSTGRES"),
                    "1",
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "KUBEJOB_TEST_POSTGRES is required for this integration test job.");
            }

            return;
        }

        var adminBuilder = new NpgsqlConnectionStringBuilder(configured)
        {
            Database = "postgres",
            Pooling = false
        };
        _adminConnectionString = adminBuilder.ConnectionString;
        _databaseName = "kubejob_test_" + Guid.NewGuid().ToString("N");

        await using (var admin = new NpgsqlConnection(_adminConnectionString))
        {
            await admin.OpenAsync();
            await using var create = admin.CreateCommand();
            create.CommandText = $"CREATE DATABASE \"{_databaseName}\"";
            await create.ExecuteNonQueryAsync();
        }

        var testBuilder = new NpgsqlConnectionStringBuilder(configured)
        {
            Database = _databaseName,
            Pooling = true
        };
        _testConnectionString = testBuilder.ConnectionString;
        new DbInitializer(_testConnectionString).Initialize();
        _dataSource = NpgsqlDataSource.Create(_testConnectionString);
        _store = new PostgreSqlJobRuntimeStore(_dataSource);
    }

    public async Task DisposeAsync()
    {
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
        }

        if (_adminConnectionString is null || _databaseName is null)
        {
            return;
        }

        await using var admin = new NpgsqlConnection(_adminConnectionString);
        await admin.OpenAsync();
        await using var terminate = admin.CreateCommand();
        terminate.CommandText =
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
            "WHERE datname = @database AND pid <> pg_backend_pid();";
        terminate.Parameters.AddWithValue("database", _databaseName);
        await terminate.ExecuteNonQueryAsync();

        await using var drop = admin.CreateCommand();
        drop.CommandText = $"DROP DATABASE IF EXISTS \"{_databaseName}\"";
        await drop.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Concurrent_reinitialization_preserves_schema_version()
    {
        if (!Enabled) return;

        await Task.WhenAll(
            Enumerable.Range(0, 3)
                .Select(_ => Task.Run(() => new DbInitializer(_testConnectionString!).Initialize())));

        await using var connection = await _dataSource!.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT Version
            FROM Kj2_SchemaMigrations
            ORDER BY Version DESC
            LIMIT 1;";

        Convert.ToInt32(await command.ExecuteScalarAsync())
            .Should().Be(DbInitializer.CurrentSchemaVersion);
    }

    [Fact]
    public async Task Batch_submit_is_atomic_idempotent_and_creates_one_managed_wake_per_new_run()
    {
        if (!Enabled) return;
        var store = _store!;

        var commands = Enumerable.Range(0, 50)
            .Select(index => NewSubmission(idempotencyKey: $"batch:{index}"))
            .Concat(new[]
            {
                NewSubmission(idempotencyKey: "batch:0"),
                NewSubmission(idempotencyKey: "batch:5")
            })
            .ToArray();

        var results = await store.SubmitBatchAsync(commands, CancellationToken.None);

        results.Should().HaveCount(52);
        results.Count(result => result.Existing).Should().Be(2);
        results[50].Run.Id.Should().Be(results[0].Run.Id);
        results[51].Run.Id.Should().Be(results[5].Run.Id);
        results.Take(50).Should().OnlyContain(result =>
            result.Run.DeliveryProfile == ExecutionDeliveryProfile.Pull
            && result.Run.TransportId == null);

        var outbox = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            100,
            CancellationToken.None);
        outbox.Should().HaveCount(50);
        outbox.Should().OnlyContain(message => message.EventType == OutboxEventTypes.WorkAvailable);
    }

    [Fact]
    public async Task Concurrent_idempotent_submit_returns_one_durable_run()
    {
        if (!Enabled) return;
        var store = _store!;

        var submissions = await Task.WhenAll(
            Enumerable.Range(0, 16)
                .Select(_ => store.SubmitAsync(
                    NewSubmission(idempotencyKey: "same-key"),
                    CancellationToken.None).AsTask()));

        submissions.Select(result => result.Run.Id).Distinct().Should().ContainSingle();
        submissions.Count(result => result.Existing).Should().Be(15);
    }

    [Fact]
    public async Task Claim_creates_attempt_and_completion_persists_terminal_state()
    {
        if (!Enabled) return;
        var store = _store!;
        var submitted = await store.SubmitAsync(NewSubmission(), CancellationToken.None);
        var session = await RegisterAsync(store, "worker-claim", "session-claim");

        var claimed = await ClaimAsync(store, session, availableSlots: 1);
        claimed.Should().ContainSingle();
        claimed[0].RunId.Should().Be(submitted.Run.Id);

        var completion = await CompleteAsync(store, session, claimed[0], JobAttemptOutcome.Succeeded);
        completion.Accepted.Should().BeTrue();
        completion.Phase.Should().Be(JobPhase.Succeeded);

        var run = await store.GetRunAsync(submitted.Run.Id, CancellationToken.None);
        run!.Phase.Should().Be(JobPhase.Succeeded);
        run.AttemptCount.Should().Be(1);
    }

    [Fact]
    public async Task New_session_epoch_fences_stale_attempt_completion()
    {
        if (!Enabled) return;
        var store = _store!;
        await store.SubmitAsync(NewSubmission(), CancellationToken.None);
        var first = await RegisterAsync(store, "worker-fence", "session-a");
        var claimed = (await ClaimAsync(store, first, 1)).Single();

        var replacement = await RegisterAsync(store, "worker-fence", "session-b");
        replacement.Epoch.Should().BeGreaterThan(first.Epoch);

        var stale = await CompleteAsync(store, first, claimed, JobAttemptOutcome.Succeeded);
        stale.Accepted.Should().BeFalse();
    }

    [Fact]
    public async Task Managed_cancel_is_observed_by_lease_renewal_without_broker_cancel_queue()
    {
        if (!Enabled) return;
        var store = _store!;
        var submitted = await store.SubmitAsync(NewSubmission(), CancellationToken.None);
        var session = await RegisterAsync(store, "worker-cancel", "session-cancel");
        var claimed = (await ClaimAsync(store, session, 1)).Single();

        var cancel = await store.RequestCancelAsync(
            submitted.Run.Id,
            "integration cancel",
            null,
            CancellationToken.None);
        cancel.Requested.Should().BeTrue();

        var renewal = await store.RenewLeasesAsync(
            new RenewLeasesRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                new[] { new LeaseRenewal(claimed.AttemptId, claimed.LeaseToken) }),
            TimeSpan.FromSeconds(30),
            CancellationToken.None);

        renewal.Should().ContainSingle();
        renewal[0].CancelRequested.Should().BeTrue();
    }

    [Fact]
    public async Task Expired_lease_is_requeued_and_can_be_claimed_by_another_worker()
    {
        if (!Enabled) return;
        var store = _store!;
        var submitted = await store.SubmitAsync(NewSubmission(maxAttempts: 2), CancellationToken.None);
        var first = await RegisterAsync(store, "worker-expire-a", "session-expire-a");
        var claimed = (await store.ClaimAsync(
            NewClaim(first, 1),
            TimeSpan.FromMilliseconds(20),
            1,
            CancellationToken.None)).Single();

        await Task.Delay(40);
        var requeued = await store.RequeueExpiredLeasesAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TestRetryPolicy,
            100,
            CancellationToken.None);
        requeued.Should().Be(1);

        var second = await RegisterAsync(store, "worker-expire-b", "session-expire-b");
        var reclaimed = await ClaimAsync(store, second, 1);
        reclaimed.Should().ContainSingle();
        reclaimed[0].RunId.Should().Be(submitted.Run.Id);
        reclaimed[0].AttemptNumber.Should().Be(claimed.AttemptNumber + 1);
    }

    [Fact]
    public async Task KeyOrdered_same_key_serializes_while_different_key_can_progress()
    {
        if (!Enabled) return;
        var store = _store!;
        var firstA = await store.SubmitAsync(
            NewSubmission(queue: "ordered", concurrencyKey: "A", orderingMode: ExecutionOrderingMode.KeyOrdered),
            CancellationToken.None);
        var secondA = await store.SubmitAsync(
            NewSubmission(queue: "ordered", concurrencyKey: "A", orderingMode: ExecutionOrderingMode.KeyOrdered),
            CancellationToken.None);
        var firstB = await store.SubmitAsync(
            NewSubmission(queue: "ordered", concurrencyKey: "B", orderingMode: ExecutionOrderingMode.KeyOrdered),
            CancellationToken.None);

        var session = await RegisterAsync(store, "worker-key", "session-key", queue: "ordered");
        var firstWave = await ClaimAsync(store, session, 3, queue: "ordered");

        firstWave.Should().HaveCount(2);
        firstWave.Select(job => job.RunId).Should().BeEquivalentTo(new[] { firstA.Run.Id, firstB.Run.Id });

        var firstAClaim = firstWave.Single(job => job.RunId == firstA.Run.Id);
        await CompleteAsync(store, session, firstAClaim, JobAttemptOutcome.Succeeded);

        var secondWave = await ClaimAsync(store, session, 1, queue: "ordered");
        secondWave.Should().ContainSingle();
        secondWave[0].RunId.Should().Be(secondA.Run.Id);
    }

    [Fact]
    public async Task StrictFifo_blocks_successor_until_predecessor_is_terminal()
    {
        if (!Enabled) return;
        var store = _store!;
        await store.SubmitAsync(
            NewSubmission(queue: "fifo", orderingMode: ExecutionOrderingMode.StrictFifo),
            CancellationToken.None);
        await store.SubmitAsync(
            NewSubmission(queue: "fifo", orderingMode: ExecutionOrderingMode.StrictFifo),
            CancellationToken.None);

        var session = await RegisterAsync(store, "worker-fifo", "session-fifo", queue: "fifo");
        var first = await ClaimAsync(store, session, 2, queue: "fifo");
        first.Should().ContainSingle();

        (await ClaimAsync(store, session, 1, queue: "fifo")).Should().BeEmpty();
        await CompleteAsync(store, session, first[0], JobAttemptOutcome.Succeeded);
        (await ClaimAsync(store, session, 1, queue: "fifo")).Should().ContainSingle();
    }

    [Fact]
    public async Task Schedule_commit_creates_managed_run_and_advances_occurrence_cursor()
    {
        if (!Enabled) return;
        var store = _store!;
        var due = DateTimeOffset.UtcNow.AddMinutes(-1);
        var schedule = NewSchedule(due);
        await store.UpsertAsync(schedule, CancellationToken.None);

        var claim = (await store.ClaimDueAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();
        var next = due.AddMinutes(5);

        var run = await store.CommitFireAsync(
            new CommitScheduleFireCommand(
                schedule.Id,
                claim.ClaimToken,
                claim.ExpectedVersion,
                due,
                next,
                true,
                "schedule-run-1",
                "schedule:integration:1"),
            CancellationToken.None);

        run.Should().NotBeNull();
        run!.DeliveryProfile.Should().Be(ExecutionDeliveryProfile.Pull);
        run.TransportId.Should().BeNull();
        run.ScheduleId.Should().Be(schedule.Id);
        run.ScheduledFor.Should().Be(due);

        var persisted = await store.GetAsync(schedule.Id, CancellationToken.None);
        persisted!.LastFireAt.Should().Be(due);
        persisted.NextFireAt.Should().Be(next);
    }

    [Fact]
    public async Task Outbox_failed_claim_can_be_reclaimed_after_retry_time()
    {
        if (!Enabled) return;
        var store = _store!;
        await store.SubmitAsync(NewSubmission(), CancellationToken.None);

        var first = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);
        first.Should().ContainSingle();

        await store.MarkFailedAsync(
            new OutboxFailure(
                first[0].Id,
                first[0].ClaimToken!,
                "synthetic",
                DateTimeOffset.UtcNow.AddMilliseconds(-1)),
            CancellationToken.None);

        var retry = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);
        retry.Should().ContainSingle();
        retry[0].Id.Should().Be(first[0].Id);
        retry[0].ClaimToken.Should().NotBe(first[0].ClaimToken);
    }

    private static SubmitJobCommand NewSubmission(
        string? idempotencyKey = null,
        string queue = "default",
        string? concurrencyKey = null,
        ExecutionOrderingMode orderingMode = ExecutionOrderingMode.Parallel,
        int maxAttempts = 3) =>
        new(
            JobKey: "test.echo",
            PayloadJson: "{\"value\":42}",
            Queue: queue,
            Priority: 0,
            AvailableAt: DateTimeOffset.UtcNow,
            IdempotencyKey: idempotencyKey,
            ConcurrencyKey: concurrencyKey,
            MaxAttempts: maxAttempts,
            TimeoutSeconds: 60,
            DeliveryTarget: new DeliveryTarget(
                ExecutionDeliveryProfile.Pull,
                "default",
                null,
                "default",
                orderingMode));

    private static async Task<WorkerSessionRecord> RegisterAsync(
        PostgreSqlJobRuntimeStore store,
        string workerId,
        string sessionId,
        string queue = "default") =>
        await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                workerId,
                sessionId,
                "integration",
                "localhost",
                8,
                new[] { queue },
                new[] { "test.echo" },
                new Dictionary<string, string>(),
                "default",
                "default"),
            CancellationToken.None);

    private static ClaimJobsRequest NewClaim(
        WorkerSessionRecord session,
        int availableSlots,
        string queue = "default") =>
        new(
            session.WorkerId,
            session.SessionId,
            session.Epoch,
            availableSlots,
            new[] { queue },
            new[] { "test.echo" },
            ConsumerGroup: "default",
            ExecutionLane: "default");

    private static async Task<IReadOnlyList<ClaimedJob>> ClaimAsync(
        PostgreSqlJobRuntimeStore store,
        WorkerSessionRecord session,
        int availableSlots,
        string queue = "default") =>
        await store.ClaimAsync(
            NewClaim(session, availableSlots, queue),
            TimeSpan.FromSeconds(30),
            Math.Max(1, availableSlots),
            CancellationToken.None);

    private static ValueTask<CompleteAttemptResponse> CompleteAsync(
        PostgreSqlJobRuntimeStore store,
        WorkerSessionRecord session,
        ClaimedJob job,
        JobAttemptOutcome outcome) =>
        store.CompleteAsync(
            new CompleteAttemptRequest(
                session.WorkerId,
                session.SessionId,
                session.Epoch,
                job.RunId,
                job.AttemptId,
                job.AttemptNumber,
                job.LeaseToken,
                outcome),
            TestRetryPolicy,
            CancellationToken.None);

    private static JobScheduleRecord NewSchedule(DateTimeOffset nextFireAt) => new()
    {
        Id = "integration-schedule",
        JobKey = "test.echo",
        PayloadJson = "{}",
        CronExpression = "*/5 * * * *",
        TimeZoneId = "UTC",
        Queue = "default",
        DeliveryProfile = ExecutionDeliveryProfile.Pull,
        ExecutionLane = "default",
        ConsumerGroup = "default",
        TransportId = null,
        OrderingMode = ExecutionOrderingMode.Parallel,
        Priority = 0,
        MisfirePolicy = MisfirePolicy.FireOnce,
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.Allow,
        MaxAttempts = 3,
        TimeoutSeconds = 60,
        Enabled = true,
        NextFireAt = nextFireAt,
        CreatedAt = DateTimeOffset.UtcNow,
        UpdatedAt = DateTimeOffset.UtcNow
    };
}
