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
    private NpgsqlDataSource? _dataSource;
    private PostgreSqlJobRuntimeStore? _store;

    private bool Enabled => _store is not null;

    public async Task InitializeAsync()
    {
        var configured = Environment.GetEnvironmentVariable("KUBEJOB_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(configured))
        {
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
        schedule!.NextFireAt.Should().Be(next);
        schedule.LastFireAt.Should().Be(due);
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

    private static SubmitJobCommand NewSubmission(string? idempotencyKey = null) => new(
        "mail.send",
        "{\"to\":\"user@example.com\"}",
        "default",
        0,
        DateTimeOffset.UtcNow,
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
