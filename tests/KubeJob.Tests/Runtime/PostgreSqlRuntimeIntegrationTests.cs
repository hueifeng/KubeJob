using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Data;
using KubeJob.Storage.PostgreSQL.Runtime;
using Npgsql;

namespace KubeJob.Tests.Runtime;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class PostgreSqlRuntimeCollection
{
    public const string Name = "postgres-runtime";
}

[Collection(PostgreSqlRuntimeCollection.Name)]
public sealed class PostgreSqlRuntimeIntegrationTests : IAsyncLifetime
{
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
        var connectionString = testBuilder.ConnectionString;
        _testConnectionString = connectionString;
        new DbInitializer(connectionString).Initialize();
        _dataSource = NpgsqlDataSource.Create(connectionString);
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
        terminate.CommandText = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = @database AND pid <> pg_backend_pid();";
        terminate.Parameters.AddWithValue("database", _databaseName);
        await terminate.ExecuteNonQueryAsync();

        await using var drop = admin.CreateCommand();
        drop.CommandText = $"DROP DATABASE IF EXISTS \"{_databaseName}\"";
        await drop.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Concurrent_reinitialization_preserves_schema_version_and_contract()
    {
        if (!Enabled) return;
        var connectionString = _testConnectionString!;

        await Task.WhenAll(
            Enumerable.Range(0, 3).Select(_ => Task.Run(
                () => new DbInitializer(connectionString).Initialize())));

        await using var connection = await _dataSource!.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT Version
            FROM Kj2_SchemaMigrations
            ORDER BY Version DESC
            LIMIT 1;";
        var version = Convert.ToInt32(await command.ExecuteScalarAsync());

        version.Should().Be(1);
    }

    [Fact]
    public async Task PostgreSql_broker_reconciliation_requeues_pending_run_only()
    {
        if (!Enabled) return;
        var store = _store!;
        var run = (await store.SubmitAsync(NewSubmission(), CancellationToken.None)).Run;
        var requeueAt = DateTimeOffset.UtcNow.AddMinutes(1);

        var scheduled = await store.RequeueWorkAvailableAsync(
            run.Id,
            requeueAt,
            CancellationToken.None);
        var messages = await store.ClaimPendingAsync(
            requeueAt,
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);

        scheduled.Should().BeTrue();
        messages.Count(message => message.PayloadJson.Contains(run.Id)).Should().Be(2);

        var canceled = await store.RequestCancelAsync(
            run.Id,
            "cancel before reconciliation",
            null,
            CancellationToken.None);
        var scheduledAfterCancel = await store.RequeueWorkAvailableAsync(
            run.Id,
            requeueAt,
            CancellationToken.None);

        canceled.Requested.Should().BeTrue();
        scheduledAfterCancel.Should().BeFalse();
    }

    [Fact]
    public async Task Concurrent_workers_create_only_one_current_attempt()
    {
        if (!Enabled) return;
        var store = _store!;
        var run = (await store.SubmitAsync(NewSubmission(), CancellationToken.None)).Run;
        var workerA = await RegisterAsync(store, "worker-a", "session-a");
        var workerB = await RegisterAsync(store, "worker-b", "session-b");

        var claims = await Task.WhenAll(
            store.ClaimAsync(Claim(workerA), TimeSpan.FromSeconds(30), 1, CancellationToken.None).AsTask(),
            store.ClaimAsync(Claim(workerB), TimeSpan.FromSeconds(30), 1, CancellationToken.None).AsTask());
        var attempts = await store.GetAttemptsAsync(run.Id, CancellationToken.None);

        claims.Sum(result => result.Count).Should().Be(1);
        attempts.Should().ContainSingle();
        (await store.GetRunAsync(run.Id, CancellationToken.None))!
            .CurrentAttemptId.Should().Be(attempts.Single().Id);
    }

    [Fact]
    public async Task New_worker_session_fences_old_completion()
    {
        if (!Enabled) return;
        var store = _store!;
        await store.SubmitAsync(NewSubmission(), CancellationToken.None);
        var oldSession = await RegisterAsync(store, "worker", "old-session");
        var claim = (await store.ClaimAsync(
            Claim(oldSession),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        await RegisterAsync(store, "worker", "new-session");

        var completion = await store.CompleteAsync(
            Completion(oldSession, claim, JobAttemptOutcome.Succeeded),
            TimeSpan.Zero,
            CancellationToken.None);

        completion.Accepted.Should().BeFalse();
    }

    [Fact]
    public async Task Schedule_fire_advances_cursor_and_writes_run_and_outbox_atomically()
    {
        if (!Enabled) return;
        var store = _store!;
        var due = DateTimeOffset.UtcNow.AddMinutes(-1);
        await store.UpsertAsync(NewSchedule(due), CancellationToken.None);
        var claim = (await store.ClaimDueAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();
        var next = due.AddMinutes(5);

        var run = await store.CommitFireAsync(
            new CommitScheduleFireCommand(
                claim.Schedule.Id,
                claim.ClaimToken,
                claim.ExpectedVersion,
                due,
                next,
                true,
                ScheduleReconcilerService.CreateOccurrenceId(claim.Schedule.Id, due),
                $"schedule:{claim.Schedule.Id}:{due.UtcDateTime.Ticks}"),
            CancellationToken.None);
        var schedule = await store.GetAsync(claim.Schedule.Id, CancellationToken.None);
        var outbox = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);

        run.Should().NotBeNull();
        schedule!.NextFireAt.Should().BeCloseTo(next, TimeSpan.FromMicroseconds(1));
        schedule.LastFireAt.Should().NotBeNull();
        schedule.LastFireAt!.Value.Should().BeCloseTo(due, TimeSpan.FromMicroseconds(1));
        outbox.Should().Contain(message => message.PayloadJson.Contains(run!.Id));
    }

    [Fact]
    public async Task Abandoned_outbox_publication_is_reclaimed()
    {
        if (!Enabled) return;
        var store = _store!;
        await store.SubmitAsync(NewSubmission(), CancellationToken.None);
        var first = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromMilliseconds(100),
            1,
            CancellationToken.None);

        await Task.Delay(150);
        var second = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None);

        second.Single().Id.Should().Be(first.Single().Id);
        second.Single().PublishAttempts.Should().Be(2);
    }

    [Fact]
    public async Task Stale_outbox_failure_cannot_overwrite_reclaimed_message()
    {
        if (!Enabled) return;
        var store = _store!;
        await store.SubmitAsync(NewSubmission(), CancellationToken.None);
        var first = (await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromMilliseconds(100),
            1,
            CancellationToken.None)).Single();
        var firstClaimToken = first.ClaimToken!;

        await Task.Delay(150);
        var second = (await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None)).Single();

        await store.MarkFailedAsync(
            new OutboxFailure(
                second.Id,
                firstClaimToken,
                "stale publisher",
                DateTimeOffset.UtcNow.AddSeconds(-1)),
            CancellationToken.None);

        var beforeCurrentLeaseExpiry = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            1,
            CancellationToken.None);

        beforeCurrentLeaseExpiry.Should().BeEmpty();
    }

    [Fact]
    public async Task Batch_outbox_publication_requires_each_matching_claim_token()
    {
        if (!Enabled) return;
        var store = _store!;
        await store.SubmitAsync(NewSubmission(idempotencyKey: "outbox-batch:one"), CancellationToken.None);
        await store.SubmitAsync(NewSubmission(idempotencyKey: "outbox-batch:two"), CancellationToken.None);
        var claimed = (await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None)).ToArray();

        await store.MarkPublishedAsync(
            new[]
            {
                new OutboxPublication(claimed[0].Id, claimed[0].ClaimToken!),
                new OutboxPublication(claimed[1].Id, "wrong-token")
            },
            DateTimeOffset.UtcNow,
            CancellationToken.None);

        var reclaimed = await store.ClaimPendingAsync(
            claimed[1].AvailableAt.AddMilliseconds(1),
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);

        reclaimed.Should().ContainSingle(message => message.Id == claimed[1].Id);
    }

    [Fact]
    public async Task Targeted_claim_admits_only_the_requested_run()
    {
        if (!Enabled) return;
        var store = _store!;
        var target = (await store.SubmitAsync(
            NewSubmission(idempotencyKey: "targeted:postgres"),
            CancellationToken.None)).Run;
        await store.SubmitAsync(
            NewSubmission(idempotencyKey: "other:postgres"),
            CancellationToken.None);
        var worker = await RegisterAsync(store, "targeted-worker", "targeted-session");

        var claimed = await store.ClaimAsync(
            Claim(worker) with { RunIds = new[] { target.Id } },
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None);

        claimed.Should().ContainSingle(job => job.RunId == target.Id);
    }

    [Fact]
    public async Task Concurrent_idempotent_submission_returns_one_logical_run()
    {
        if (!Enabled) return;
        var store = _store!;
        var command = NewSubmission(idempotencyKey: "welcome:postgres:42");

        var submissions = await Task.WhenAll(
            store.SubmitAsync(command, CancellationToken.None).AsTask(),
            store.SubmitAsync(command, CancellationToken.None).AsTask());

        submissions.Select(result => result.Run.Id).Distinct().Should().ContainSingle();
        submissions.Count(result => result.Existing).Should().Be(1);
    }

    [Fact]
    public async Task Dashboard_overview_reports_ready_age_and_recent_activity()
    {
        if (!Enabled) return;
        var store = _store!;
        await store.SubmitAsync(NewSubmission(), CancellationToken.None);
        var worker = await RegisterAsync(store, "dashboard-worker", "dashboard-session");
        var completedClaim = (await store.ClaimAsync(
            Claim(worker),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        var completion = await store.CompleteAsync(
            Completion(worker, completedClaim, JobAttemptOutcome.Succeeded),
            TimeSpan.Zero,
            CancellationToken.None);

        var readyAt = DateTimeOffset.UtcNow.AddMinutes(-7);
        await store.SubmitAsync(
            NewSubmission(queue: "dashboard-mail", availableAt: readyAt),
            CancellationToken.None);
        await store.SubmitAsync(
            NewSubmission(
                queue: "dashboard-future",
                availableAt: DateTimeOffset.UtcNow.AddMinutes(10)),
            CancellationToken.None);

        var overview = await store.GetOverviewAsync(10, CancellationToken.None);
        var readyQueue = overview.Queues.Single(queue => queue.Queue == "dashboard-mail");
        var futureQueue = overview.Queues.Single(queue => queue.Queue == "dashboard-future");

        completion.Accepted.Should().BeTrue();
        overview.LastHour.SucceededRuns.Should().BeGreaterThanOrEqualTo(1);
        readyQueue.PendingRuns.Should().Be(1);
        readyQueue.OldestReadyAt.Should().BeCloseTo(readyAt, TimeSpan.FromMilliseconds(1));
        futureQueue.OldestReadyAt.Should().BeNull();
        overview.ObservedAt.Should().BeAfter(readyAt);
    }

    private static SubmitJobCommand NewSubmission(
        string? idempotencyKey = null,
        string queue = "default",
        DateTimeOffset? availableAt = null) => new(
        "mail.send",
        "{\"to\":\"user@example.com\"}",
        queue,
        0,
        availableAt ?? DateTimeOffset.UtcNow,
        idempotencyKey,
        null,
        3,
        60);

    private static JobScheduleRecord NewSchedule(DateTimeOffset nextFireAt) => new()
    {
        Id = "postgres-schedule",
        JobKey = "mail.send",
        PayloadJson = "{\"to\":\"scheduled@example.com\"}",
        CronExpression = "*/5 * * * *",
        TimeZoneId = "UTC",
        Queue = "default",
        MisfirePolicy = MisfirePolicy.FireOnce,
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.Allow,
        MaxAttempts = 3,
        TimeoutSeconds = 60,
        Enabled = true,
        NextFireAt = nextFireAt,
        CreatedAt = DateTimeOffset.UtcNow,
        UpdatedAt = DateTimeOffset.UtcNow
    };

    private static ValueTask<WorkerSessionRecord> RegisterAsync(
        PostgreSqlJobRuntimeStore store,
        string workerId,
        string sessionId) => store.RegisterAsync(
        new RegisterWorkerSessionRequest(
            workerId,
            sessionId,
            "integration-test",
            "localhost",
            1,
            new[] { "default" },
            new[] { "mail.send" },
            new Dictionary<string, string>()),
        CancellationToken.None);

    private static ClaimJobsRequest Claim(WorkerSessionRecord session) => new(
        session.WorkerId,
        session.SessionId,
        session.Epoch,
        1,
        new[] { "default" },
        new[] { "mail.send" });

    private static CompleteAttemptRequest Completion(
        WorkerSessionRecord session,
        ClaimedJob job,
        JobAttemptOutcome outcome) => new(
        session.WorkerId,
        session.SessionId,
        session.Epoch,
        job.RunId,
        job.AttemptId,
        job.AttemptNumber,
        job.LeaseToken,
        outcome);
}
