using System.Diagnostics;
using System.Text.Json;
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
    private static readonly RetryPolicy TestRetryPolicy =
        new(BackoffStrategy.Fixed, TimeSpan.Zero, TimeSpan.Zero);
    private static readonly RetryPolicy TestRetryPolicyLong =
        new(BackoffStrategy.Fixed, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

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

        version.Should().Be(DbInitializer.CurrentSchemaVersion);
    }

    [Fact]
    public async Task Batch_submit_persists_runs_and_honors_idempotency_in_one_transaction()
    {
        if (!Enabled) return;
        var store = _store!;

        var commands = new List<SubmitJobCommand>(202);
        for (var index = 0; index < 200; index++)
        {
            commands.Add(NewSubmission(idempotencyKey: $"batch:{index}", deliveryTarget: BrokerDispatchTarget()));
        }

        // Duplicate idempotency keys must resolve to the first occurrence with
        // Existing=true and must not write a second outbox row.
        commands.Add(NewSubmission(idempotencyKey: "batch:0", deliveryTarget: BrokerDispatchTarget()));
        commands.Add(NewSubmission(idempotencyKey: "batch:5", deliveryTarget: BrokerDispatchTarget()));

        var results = await store.SubmitBatchAsync(commands, CancellationToken.None);

        results.Should().HaveCount(202);
        results.Count(x => x.Existing).Should().Be(2);
        results[200].Existing.Should().BeTrue();
        results[201].Existing.Should().BeTrue();
        results[200].Run.Id.Should().Be(results[0].Run.Id);
        results[201].Run.Id.Should().Be(results[5].Run.Id);

        var newRunIds = results.Take(200).Select(x => x.Run.Id).ToArray();
        foreach (var runId in newRunIds)
        {
            var run = await store.GetRunAsync(runId, CancellationToken.None);
            run.Should().NotBeNull();
            run!.Phase.Should().Be(JobPhase.Pending);
        }

        // Every newly inserted run gets exactly one work-available outbox row;
        // the two duplicates contribute none.
        var outbox = await store.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            500,
            CancellationToken.None);
        var workAvailable = outbox
            .Where(x => string.Equals(x.EventType, "work-available", StringComparison.Ordinal))
            .ToArray();
        workAvailable.Should().HaveCount(200);
        foreach (var runId in newRunIds)
        {
            workAvailable.Count(x => x.PayloadJson.Contains(runId)).Should().Be(1);
        }
    }

    [Fact]
    public async Task Batch_submit_persists_continuation_and_compensation()
    {
        if (!Enabled) return;
        var store = _store!;

        var continuation = new Continuation
        {
            JobKey = "mail.send.followup",
            PayloadJson = "{\"parent\":\"batch-cont\"}",
            Trigger = ContinuationTrigger.OnSuccess
        };
        var compensation = new Compensation
        {
            JobKey = "mail.send.compensate",
            PayloadJson = "{\"parent\":\"batch-comp\"}"
        };

        var results = await store.SubmitBatchAsync(new[]
        {
            NewSubmission(idempotencyKey: "batch-continuation:1") with
            {
                Continuation = continuation,
                Compensation = compensation
            }
        }, CancellationToken.None);

        results.Should().HaveCount(1);
        results[0].Existing.Should().BeFalse();

        var run = await store.GetRunAsync(results[0].Run.Id, CancellationToken.None);
        run.Should().NotBeNull();
        run!.Continuation.Should().BeEquivalentTo(continuation);
        run.Compensation.Should().BeEquivalentTo(compensation);
    }

    [Fact]
    public async Task GetByIdempotencyKeyAsync_returns_active_run_for_matching_key()
    {
        if (!Enabled) return;
        var store = _store!;

        var submitted = await store.SubmitAsync(
            NewSubmission(idempotencyKey: "lookup-by-key"),
            CancellationToken.None);

        var found = await store.GetByIdempotencyKeyAsync("lookup-by-key", CancellationToken.None);
        found.Should().NotBeNull();
        found!.Id.Should().Be(submitted.Run.Id);
    }

    [Fact]
    public async Task SubmitAsync_round_trips_continuation_and_compensation()
    {
        if (!Enabled) return;
        var store = _store!;

        var continuation = new Continuation
        {
            JobKey = "mail.send.followup",
            PayloadJson = "{\"parent\":\"single\"}",
            Trigger = ContinuationTrigger.OnSuccess
        };
        var compensation = new Compensation
        {
            JobKey = "mail.send.compensate",
            PayloadJson = "{\"parent\":\"single-comp\"}"
        };

        var submitted = await store.SubmitAsync(
            NewSubmission(idempotencyKey: "single-continuation") with
            {
                Continuation = continuation,
                Compensation = compensation
            },
            CancellationToken.None);

        submitted.Run.Continuation.Should().BeEquivalentTo(continuation);
        submitted.Run.Compensation.Should().BeEquivalentTo(compensation);

        var readBack = await store.GetRunAsync(submitted.Run.Id, CancellationToken.None);
        readBack!.Continuation.Should().BeEquivalentTo(continuation);
        readBack.Compensation.Should().BeEquivalentTo(compensation);
    }

    [Fact]
    public async Task Completion_fires_continuation_on_success_with_parent_context()
    {
        if (!Enabled) return;
        var store = _store!;

        var parent = (await store.SubmitAsync(
            NewSubmission(idempotencyKey: "cont-on-success") with
            {
                Continuation = new Continuation
                {
                    JobKey = "mail.send.followup",
                    PayloadJson = "{\"parent\":\"cont-on-success\"}",
                    Trigger = ContinuationTrigger.OnSuccess
                },
                Compensation = new Compensation { JobKey = "must.not.fire" }
            },
            CancellationToken.None)).Run;

        var worker = await RegisterAsync(store, "cont-success-worker", "cont-success-session");
        var claim = (await store.ClaimAsync(
            Claim(worker),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        claim.JobKey.Should().Be("mail.send");

        var completion = await store.CompleteAsync(
            Completion(worker, claim, JobAttemptOutcome.Succeeded),
            TestRetryPolicy,
            CancellationToken.None);
        completion.Accepted.Should().BeTrue();
        (await store.GetRunAsync(parent.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Succeeded);

        // The continuation run must be claimable under its own job key, with
        // the parent's execution context and the configured payload. The
        // follow-up session registers the continuation job key as a capability.
        var followUpWorker = await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "cont-success-worker",
                "cont-success-followup-session",
                "integration-test",
                "localhost",
                1,
                new[] { "default" },
                new[] { "mail.send.followup" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        var followUp = (await store.ClaimAsync(
            new ClaimJobsRequest(
                followUpWorker.WorkerId,
                followUpWorker.SessionId,
                followUpWorker.Epoch,
                1,
                new[] { "default" },
                new[] { "mail.send.followup" }),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        followUp.JobKey.Should().Be("mail.send.followup");

        var followUpRun = (await store.GetRunAsync(followUp.RunId, CancellationToken.None))!;
        JsonSerializer.Deserialize<Dictionary<string, string?>>(followUpRun.PayloadJson)!
            .Should().Equal(new Dictionary<string, string?> { ["parent"] = "cont-on-success" });
        followUpRun.DeliveryProfile.Should().Be(parent.DeliveryProfile);
        followUpRun.MaxAttempts.Should().Be(parent.MaxAttempts);
        followUpRun.ParentRunId.Should().Be(parent.Id);
        followUpRun.RelationKind.Should().Be(RunRelationKind.Continuation);

        // Compensation must not fire on success.
        var compensationWorker = await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "cont-success-worker",
                "cont-success-compensation-session",
                "integration-test",
                "localhost",
                1,
                new[] { "default" },
                new[] { "must.not.fire" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        var compensationClaim = await store.ClaimAsync(
            new ClaimJobsRequest(
                compensationWorker.WorkerId,
                compensationWorker.SessionId,
                compensationWorker.Epoch,
                1,
                new[] { "default" },
                new[] { "must.not.fire" }),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None);
        compensationClaim.Should().BeEmpty();
    }

    [Fact]
    public async Task Completion_fires_compensation_on_permanent_failure()
    {
        if (!Enabled) return;
        var store = _store!;

        var parent = (await store.SubmitAsync(
            NewSubmission(idempotencyKey: "comp-on-failure") with
            {
                Compensation = new Compensation
                {
                    JobKey = "mail.send.compensate",
                    PayloadJson = "{\"parent\":\"comp-on-failure\"}"
                },
                Continuation = new Continuation
                {
                    JobKey = "mail.send.followup",
                    Trigger = ContinuationTrigger.OnAnyTerminal
                }
            },
            CancellationToken.None)).Run;

        var worker = await RegisterAsync(store, "comp-failure-worker", "comp-failure-session");
        var claim = (await store.ClaimAsync(
            Claim(worker),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();

        var completion = await store.CompleteAsync(
            Completion(worker, claim, JobAttemptOutcome.PermanentFailure),
            TestRetryPolicy,
            CancellationToken.None);
        completion.Accepted.Should().BeTrue();
        (await store.GetRunAsync(parent.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Failed);

        // Both compensation (PermanentFailure) and OnAnyTerminal continuation
        // fire, each under its own job key. A follow-up-capable session is
        // registered for the compensation and continuation keys.
        var followUpWorker = await store.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "comp-failure-worker",
                "comp-failure-followup-session",
                "integration-test",
                "localhost",
                2,
                new[] { "default" },
                new[] { "mail.send.compensate", "mail.send.followup" },
                new Dictionary<string, string>()),
            CancellationToken.None);
        var compensationClaim = (await store.ClaimAsync(
            new ClaimJobsRequest(
                followUpWorker.WorkerId,
                followUpWorker.SessionId,
                followUpWorker.Epoch,
                1,
                new[] { "default" },
                new[] { "mail.send.compensate" }),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        compensationClaim.JobKey.Should().Be("mail.send.compensate");
        JsonSerializer.Deserialize<Dictionary<string, string?>>(
            (await store.GetRunAsync(compensationClaim.RunId, CancellationToken.None))!.PayloadJson)!
            .Should().Equal(new Dictionary<string, string?> { ["parent"] = "comp-on-failure" });
        var compensationRun = (await store.GetRunAsync(compensationClaim.RunId, CancellationToken.None))!;
        compensationRun.ParentRunId.Should().Be(parent.Id);
        compensationRun.RelationKind.Should().Be(RunRelationKind.Compensation);

        var continuationClaim = (await store.ClaimAsync(
            new ClaimJobsRequest(
                followUpWorker.WorkerId,
                followUpWorker.SessionId,
                followUpWorker.Epoch,
                1,
                new[] { "default" },
                new[] { "mail.send.followup" }),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        continuationClaim.JobKey.Should().Be("mail.send.followup");
        var continuationRun = (await store.GetRunAsync(continuationClaim.RunId, CancellationToken.None))!;
        continuationRun.ParentRunId.Should().Be(parent.Id);
        continuationRun.RelationKind.Should().Be(RunRelationKind.Continuation);
    }

    [Fact]
    public async Task Batch_submit_is_faster_than_single_submits()
    {
        if (!Enabled) return;
        if (Environment.GetEnvironmentVariable("KUBEJOB_PERF") is null) return;
        var store = _store!;

        const int count = 300;
        var singleCommands = Enumerable.Range(0, count)
            .Select(i => NewSubmission(idempotencyKey: $"perf-single:{i}"))
            .ToArray();
        var batchCommands = Enumerable.Range(0, count)
            .Select(i => NewSubmission(idempotencyKey: $"perf-batch:{i}"))
            .ToArray();

        var singleStart = Stopwatch.GetTimestamp();
        foreach (var command in singleCommands)
        {
            await store.SubmitAsync(command, CancellationToken.None);
        }
        var singleElapsed = Stopwatch.GetElapsedTime(singleStart);

        var batchStart = Stopwatch.GetTimestamp();
        var results = await store.SubmitBatchAsync(batchCommands, CancellationToken.None);
        var batchElapsed = Stopwatch.GetElapsedTime(batchStart);

        results.Should().HaveCount(count);
        batchElapsed.Should().BeLessThan(singleElapsed,
            "batch submit should amortize round trips and WAL flushes");
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
        messages.Count(message => message.PayloadJson.Contains(run.Id)).Should().Be(1);

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
            TestRetryPolicy,
            CancellationToken.None);

        completion.Accepted.Should().BeFalse();
    }

    [Fact]
    public async Task Completion_batch_atomically_finishes_multiple_successful_attempts()
    {
        if (!Enabled) return;
        var store = _store!;
        foreach (var index in Enumerable.Range(0, 3))
        {
            await store.SubmitAsync(
                NewSubmission($"batch-completion-{index}"),
                CancellationToken.None);
        }

        var worker = await RegisterAsync(store, "batch-worker", "batch-session", 3);
        var claims = await store.ClaimAsync(
            new ClaimJobsRequest(
                worker.WorkerId,
                worker.SessionId,
                worker.Epoch,
                3,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            3,
            CancellationToken.None);

        claims.Should().HaveCount(3);
        var completions = await store.CompleteBatchAsync(
            claims.Select(job => Completion(worker, job, JobAttemptOutcome.Succeeded)).ToArray(),
            TestRetryPolicy,
            CancellationToken.None);

        completions.Should().OnlyContain(result => result.Accepted);
        foreach (var job in claims)
        {
            (await store.GetRunAsync(job.RunId, CancellationToken.None))!
                .Phase.Should().Be(JobPhase.Succeeded);
        }
    }

    [Fact]
    public async Task Completion_batch_handles_mixed_outcomes_in_a_single_call()
    {
        if (!Enabled) return;
        var store = _store!;
        var succeededRun = (await store.SubmitAsync(
            NewSubmission("batch-mixed:succeeded"),
            CancellationToken.None)).Run;
        var failedRun = (await store.SubmitAsync(
            NewSubmission("batch-mixed:failed"),
            CancellationToken.None)).Run;
        var retryRun = (await store.SubmitAsync(
            NewSubmission("batch-mixed:retry"),
            CancellationToken.None)).Run;
        var deadRun = (await store.SubmitAsync(
            NewSubmission("batch-mixed:dead", maxAttempts: 1),
            CancellationToken.None)).Run;
        var canceledRun = (await store.SubmitAsync(
            NewSubmission("batch-mixed:canceled"),
            CancellationToken.None)).Run;

        var worker = await RegisterAsync(store, "batch-mixed-worker", "batch-mixed-session", 5);
        var claims = await store.ClaimAsync(
            new ClaimJobsRequest(
                worker.WorkerId,
                worker.SessionId,
                worker.Epoch,
                5,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            5,
            CancellationToken.None);

        claims.Should().HaveCount(5);
        var claimsByRunId = claims.ToDictionary(job => job.RunId);

        (await store.RequestCancelAsync(
            canceledRun.Id,
            "cancel while batch completes",
            null,
            CancellationToken.None)).Requested.Should().BeTrue();

        var requests = new[]
        {
            Completion(worker, claimsByRunId[succeededRun.Id], JobAttemptOutcome.Succeeded),
            Completion(worker, claimsByRunId[failedRun.Id], JobAttemptOutcome.PermanentFailure),
            Completion(worker, claimsByRunId[retryRun.Id], JobAttemptOutcome.RetryableFailure),
            Completion(worker, claimsByRunId[deadRun.Id], JobAttemptOutcome.RetryableFailure),
            Completion(worker, claimsByRunId[canceledRun.Id], JobAttemptOutcome.Succeeded),
        };

        var completions = await store.CompleteBatchAsync(
            requests,
            TestRetryPolicyLong,
            CancellationToken.None);

        completions.Should().OnlyContain(result => result.Accepted);
        (await store.GetRunAsync(succeededRun.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Succeeded);
        (await store.GetRunAsync(failedRun.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Failed);
        (await store.GetRunAsync(retryRun.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Pending);
        (await store.GetRunAsync(deadRun.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Dead);
        (await store.GetRunAsync(canceledRun.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Canceled);
    }

    [Fact]
    public async Task Completion_batch_fires_terminal_actions_for_each_terminal_run()
    {
        if (!Enabled) return;
        var store = _store!;
        var succeededRun = (await store.SubmitAsync(
            NewSubmission("batch-action:succeeded") with
            {
                Continuation = new Continuation
                {
                    JobKey = "mail.batch.followup",
                    PayloadJson = "{\"source\":\"batch-success\"}",
                    Trigger = ContinuationTrigger.OnSuccess
                }
            },
            CancellationToken.None)).Run;
        var failedRun = (await store.SubmitAsync(
            NewSubmission("batch-action:failed") with
            {
                Compensation = new Compensation
                {
                    JobKey = "mail.batch.compensate",
                    PayloadJson = "{\"source\":\"batch-failure\"}"
                }
            },
            CancellationToken.None)).Run;

        var worker = await RegisterAsync(store, "batch-action-worker", "batch-action-session", 2);
        var claims = await store.ClaimAsync(
            Claim(worker) with { AvailableSlots = 2 },
            TimeSpan.FromMinutes(1),
            2,
            CancellationToken.None);
        claims.Should().HaveCount(2);
        var claimsByRunId = claims.ToDictionary(job => job.RunId);

        var completions = await store.CompleteBatchAsync(
            new[]
            {
                Completion(worker, claimsByRunId[succeededRun.Id], JobAttemptOutcome.Succeeded),
                Completion(worker, claimsByRunId[failedRun.Id], JobAttemptOutcome.PermanentFailure)
            },
            TestRetryPolicy,
            CancellationToken.None);

        completions.Should().OnlyContain(result => result.Accepted);
        var followUpWorker = await RegisterAsync(
            store,
            "batch-action-followup-worker",
            "batch-action-followup-session",
            2,
            capabilities: new[] { "mail.batch.followup", "mail.batch.compensate" });
        var followUps = await store.ClaimAsync(
            Claim(followUpWorker) with
            {
                Capabilities = new[] { "mail.batch.followup", "mail.batch.compensate" },
                AvailableSlots = 2
            },
            TimeSpan.FromMinutes(1),
            2,
            CancellationToken.None);

        followUps.Select(job => job.JobKey)
            .Should().BeEquivalentTo(new[] { "mail.batch.followup", "mail.batch.compensate" });
    }

    [Fact]
    public async Task Completion_batch_honors_cancel_requested_before_success()
    {
        if (!Enabled) return;
        var store = _store!;
        var run = (await store.SubmitAsync(
            NewSubmission("batch-completion-cancel"),
            CancellationToken.None)).Run;
        var worker = await RegisterAsync(store, "batch-cancel-worker", "batch-cancel-session");
        var claim = (await store.ClaimAsync(
            Claim(worker),
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();

        (await store.RequestCancelAsync(
            run.Id,
            "cancel while handler is finishing",
            null,
            CancellationToken.None)).Requested.Should().BeTrue();

        var response = (await store.CompleteBatchAsync(
            new[] { Completion(worker, claim, JobAttemptOutcome.Succeeded) },
            TestRetryPolicy,
            CancellationToken.None)).Single();

        response.Accepted.Should().BeTrue();
        response.Phase.Should().Be(JobPhase.Canceled);
        (await store.GetRunAsync(run.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Canceled);
    }

    [Fact]
    public async Task Batch_lease_renewal_commits_matching_attempts_and_rejects_mismatched_token()
    {
        if (!Enabled) return;
        var store = _store!;
        foreach (var index in Enumerable.Range(0, 3))
        {
            await store.SubmitAsync(
                NewSubmission($"batch-renewal-{index}"),
                CancellationToken.None);
        }

        var worker = await RegisterAsync(store, "renewal-worker", "renewal-session", 3);
        var claims = await store.ClaimAsync(
            new ClaimJobsRequest(
                worker.WorkerId,
                worker.SessionId,
                worker.Epoch,
                3,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            3,
            CancellationToken.None);

        claims.Should().HaveCount(3);
        var renewals = claims
            .Select((job, index) => new LeaseRenewal(
                job.AttemptId,
                index == 0 ? "wrong-token" : job.LeaseToken))
            .ToArray();

        var results = await store.RenewLeasesAsync(
            new RenewLeasesRequest(worker.WorkerId, worker.SessionId, worker.Epoch, renewals),
            TimeSpan.FromMinutes(1),
            CancellationToken.None);

        results.Should().HaveCount(3);
        results.Single(r => r.AttemptId == claims[0].AttemptId).Renewed.Should().BeFalse();
        results.Where(r => r.AttemptId != claims[0].AttemptId)
            .Should().OnlyContain(r => r.Renewed);
    }

    [Fact]
    public async Task Batch_lease_expiry_reconciles_cancel_retry_and_dead_letter_outcomes_in_one_pass()
    {
        if (!Enabled) return;
        var store = _store!;

        var retryRun = (await store.SubmitAsync(
            NewSubmission(idempotencyKey: "lease-expiry:retry"),
            CancellationToken.None)).Run;
        var deadRun = (await store.SubmitAsync(
            NewSubmission(idempotencyKey: "lease-expiry:dead", maxAttempts: 1),
            CancellationToken.None)).Run;
        var cancelRun = (await store.SubmitAsync(
            NewSubmission(idempotencyKey: "lease-expiry:cancel"),
            CancellationToken.None)).Run;

        var worker = await RegisterAsync(store, "lease-expiry-worker", "lease-expiry-session", 3);
        var claims = await store.ClaimAsync(
            new ClaimJobsRequest(
                worker.WorkerId,
                worker.SessionId,
                worker.Epoch,
                3,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMilliseconds(50),
            3,
            CancellationToken.None);

        claims.Should().HaveCount(3);
        (await store.RequestCancelAsync(
            cancelRun.Id,
            "cancel before lease expiry",
            null,
            CancellationToken.None)).Requested.Should().BeTrue();

        await Task.Delay(150);
        var reconciled = await store.RequeueExpiredLeasesAsync(
            DateTimeOffset.UtcNow,
            TestRetryPolicyLong,
            10,
            CancellationToken.None);

        reconciled.Should().Be(3);
        (await store.GetRunAsync(retryRun.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Pending);
        (await store.GetRunAsync(deadRun.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Dead);
        (await store.GetRunAsync(cancelRun.Id, CancellationToken.None))!
            .Phase.Should().Be(JobPhase.Canceled);
    }

    [Fact]
    public async Task Lease_reaper_fires_terminal_actions_when_a_run_becomes_dead()
    {
        if (!Enabled) return;
        var store = _store!;
        var deadRun = (await store.SubmitAsync(
            NewSubmission("lease-action:dead", maxAttempts: 1) with
            {
                Continuation = new Continuation
                {
                    JobKey = "mail.lease.followup",
                    Trigger = ContinuationTrigger.OnAnyTerminal
                },
                Compensation = new Compensation
                {
                    JobKey = "mail.lease.compensate"
                }
            },
            CancellationToken.None)).Run;

        var worker = await RegisterAsync(store, "lease-action-worker", "lease-action-session");
        (await store.ClaimAsync(
            Claim(worker),
            TimeSpan.FromMilliseconds(50),
            1,
            CancellationToken.None)).Should().ContainSingle();
        await Task.Delay(150);

        (await store.RequeueExpiredLeasesAsync(
            DateTimeOffset.UtcNow,
            TestRetryPolicy,
            10,
            CancellationToken.None)).Should().Be(1);
        (await store.GetRunAsync(deadRun.Id, CancellationToken.None))!.Phase.Should().Be(JobPhase.Dead);

        var followUpWorker = await RegisterAsync(
            store,
            "lease-action-followup-worker",
            "lease-action-followup-session",
            2,
            capabilities: new[] { "mail.lease.followup", "mail.lease.compensate" });
        var followUps = await store.ClaimAsync(
            Claim(followUpWorker) with
            {
                Capabilities = new[] { "mail.lease.followup", "mail.lease.compensate" },
                AvailableSlots = 2
            },
            TimeSpan.FromMinutes(1),
            2,
            CancellationToken.None);

        followUps.Select(job => job.JobKey)
            .Should().BeEquivalentTo(new[] { "mail.lease.followup", "mail.lease.compensate" });
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
        run!.ConcurrencyKey.Should().Be("schedule:postgres:42");
        run.RetryPolicy.Should().BeEquivalentTo(schedule!.RetryPolicy);
        run.Continuation.Should().BeEquivalentTo(schedule.Continuation);
        run.Compensation.Should().BeEquivalentTo(schedule.Compensation);
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
        await store.SubmitAsync(NewSubmission(deliveryTarget: BrokerDispatchTarget()), CancellationToken.None);
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
        await store.SubmitAsync(NewSubmission(deliveryTarget: BrokerDispatchTarget()), CancellationToken.None);
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
        await store.SubmitAsync(
            NewSubmission(idempotencyKey: "outbox-batch:one", deliveryTarget: BrokerDispatchTarget()),
            CancellationToken.None);
        await store.SubmitAsync(
            NewSubmission(idempotencyKey: "outbox-batch:two", deliveryTarget: BrokerDispatchTarget()),
            CancellationToken.None);
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
    public async Task Idempotency_conflicts_when_persisted_retry_or_terminal_action_changes()
    {
        if (!Enabled) return;
        var store = _store!;
        var first = await store.SubmitAsync(
            NewSubmission("idempotency-behavior") with
            {
                RetryPolicy = new RetryPolicy(
                    BackoffStrategy.Fixed,
                    TimeSpan.FromSeconds(1),
                    TimeSpan.FromSeconds(1)),
                Continuation = new Continuation
                {
                    JobKey = "mail.idempotency.followup",
                    PayloadJson = "{\"version\":1}"
                }
            },
            CancellationToken.None);

        var action = async () => await store.SubmitAsync(
            NewSubmission("idempotency-behavior") with
            {
                RetryPolicy = new RetryPolicy(
                    BackoffStrategy.Fixed,
                    TimeSpan.FromSeconds(2),
                    TimeSpan.FromSeconds(2)),
                Continuation = new Continuation
                {
                    JobKey = "mail.idempotency.followup",
                    PayloadJson = "{\"version\":2}"
                }
            },
            CancellationToken.None);

        var exception = await action.Should().ThrowAsync<IdempotencyConflictException>();
        exception.Which.ExistingJobId.Should().Be(first.Run.Id);
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
            TestRetryPolicy,
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

    [Fact]
    public async Task Exponential_retry_policy_grows_delay_with_attempt_count()
    {
        if (!Enabled) return;
        var store = _store!;
        var run = (await store.SubmitAsync(
            NewSubmission("exponential-backoff", maxAttempts: 3),
            CancellationToken.None)).Run;
        var worker = await RegisterAsync(store, "backoff-worker", "backoff-session");
        var exponentialPolicy = new RetryPolicy(
            BackoffStrategy.Exponential,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMinutes(10),
            Multiplier: 4.0);

        var firstClaim = (await store.ClaimAsync(Claim(worker), TimeSpan.FromMinutes(1), 1, CancellationToken.None)).Single();
        var beforeFirstRetry = DateTimeOffset.UtcNow;
        await store.CompleteAsync(
            Completion(worker, firstClaim, JobAttemptOutcome.RetryableFailure),
            exponentialPolicy,
            CancellationToken.None);
        var firstDelay = (await store.GetRunAsync(run.Id, CancellationToken.None))!.AvailableAt - beforeFirstRetry;

        await Task.Delay(1500);
        var secondClaim = (await store.ClaimAsync(Claim(worker), TimeSpan.FromMinutes(1), 1, CancellationToken.None)).Single();
        var beforeSecondRetry = DateTimeOffset.UtcNow;
        await store.CompleteAsync(
            Completion(worker, secondClaim, JobAttemptOutcome.RetryableFailure),
            exponentialPolicy,
            CancellationToken.None);
        var secondDelay = (await store.GetRunAsync(run.Id, CancellationToken.None))!.AvailableAt - beforeSecondRetry;

        secondDelay.Should().BeGreaterThan(firstDelay);
    }

    [Fact]
    public async Task Different_workers_can_complete_concurrently_without_serializing_on_a_shared_lock()
    {
        if (!Enabled) return;
        var store = _store!;
        await store.SubmitAsync(NewSubmission("concurrent-worker-a"), CancellationToken.None);
        await store.SubmitAsync(NewSubmission("concurrent-worker-b"), CancellationToken.None);
        var workerA = await RegisterAsync(store, "concurrent-a", "concurrent-a-session");
        var workerB = await RegisterAsync(store, "concurrent-b", "concurrent-b-session");
        var claimA = (await store.ClaimAsync(Claim(workerA), TimeSpan.FromMinutes(1), 1, CancellationToken.None)).Single();
        var claimB = (await store.ClaimAsync(Claim(workerB), TimeSpan.FromMinutes(1), 1, CancellationToken.None)).Single();

        var completions = await Task.WhenAll(
            store.CompleteAsync(Completion(workerA, claimA, JobAttemptOutcome.Succeeded), TestRetryPolicy, CancellationToken.None).AsTask(),
            store.CompleteAsync(Completion(workerB, claimB, JobAttemptOutcome.Succeeded), TestRetryPolicy, CancellationToken.None).AsTask());

        completions.Should().OnlyContain(result => result.Accepted);
    }

    [Fact]
    public async Task Batch_claim_admits_only_the_requested_limit_and_leaves_remainder_pending()
    {
        if (!Enabled) return;
        var store = _store!;
        var runs = new List<JobRunRecord>();
        foreach (var index in Enumerable.Range(0, 5))
        {
            runs.Add((await store.SubmitAsync(
                NewSubmission($"partial-batch-{index}"),
                CancellationToken.None)).Run);
        }

        var worker = await RegisterAsync(store, "partial-batch-worker", "partial-batch-session", 5);
        var claimed = await store.ClaimAsync(
            new ClaimJobsRequest(
                worker.WorkerId,
                worker.SessionId,
                worker.Epoch,
                5,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            3,
            CancellationToken.None);

        claimed.Should().HaveCount(3);
        var claimedRunIds = claimed.Select(job => job.RunId).ToHashSet(StringComparer.Ordinal);
        var unclaimedRunIds = runs.Select(run => run.Id).Where(id => !claimedRunIds.Contains(id));
        foreach (var runId in unclaimedRunIds)
        {
            var run = await store.GetRunAsync(runId, CancellationToken.None);
            run!.Phase.Should().Be(JobPhase.Pending);
        }

        var attemptCounts = await Task.WhenAll(
            runs.Select(run => store.GetAttemptsAsync(run.Id, CancellationToken.None).AsTask()));
        attemptCounts.Sum(attempts => attempts.Count).Should().Be(3);
    }

    [Fact]
    public async Task Batch_claim_admits_candidates_in_descending_priority_order()
    {
        if (!Enabled) return;
        var store = _store!;
        var low = (await store.SubmitAsync(
            NewSubmission("priority-low", priority: 0),
            CancellationToken.None)).Run;
        var high = (await store.SubmitAsync(
            NewSubmission("priority-high", priority: 10),
            CancellationToken.None)).Run;
        var medium = (await store.SubmitAsync(
            NewSubmission("priority-medium", priority: 5),
            CancellationToken.None)).Run;

        var worker = await RegisterAsync(store, "priority-worker", "priority-session", 2);
        var claimed = await store.ClaimAsync(
            new ClaimJobsRequest(
                worker.WorkerId,
                worker.SessionId,
                worker.Epoch,
                2,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            2,
            CancellationToken.None);

        claimed.Select(job => job.RunId).Should().Equal(high.Id, medium.Id);
        (await store.GetRunAsync(low.Id, CancellationToken.None))!.Phase.Should().Be(JobPhase.Pending);
    }

    [Fact]
    public async Task Batch_claim_admits_only_one_run_per_concurrency_key()
    {
        if (!Enabled) return;
        var store = _store!;
        const string concurrencyKey = "batch-concurrency-key";
        var first = (await store.SubmitAsync(
            NewSubmission("concurrency-batch-1", concurrencyKey: concurrencyKey),
            CancellationToken.None)).Run;
        var second = (await store.SubmitAsync(
            NewSubmission("concurrency-batch-2", concurrencyKey: concurrencyKey),
            CancellationToken.None)).Run;
        var unrelated = (await store.SubmitAsync(
            NewSubmission("concurrency-batch-unrelated"),
            CancellationToken.None)).Run;

        var worker = await RegisterAsync(store, "concurrency-batch-worker", "concurrency-batch-session", 3);
        var claimed = await store.ClaimAsync(
            new ClaimJobsRequest(
                worker.WorkerId,
                worker.SessionId,
                worker.Epoch,
                3,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            3,
            CancellationToken.None);

        claimed.Should().HaveCount(2);
        claimed.Select(job => job.RunId).Should().Contain(unrelated.Id);
        var claimedFromPair = claimed.Select(job => job.RunId)
            .Intersect(new[] { first.Id, second.Id })
            .ToArray();
        claimedFromPair.Should().ContainSingle();
        var otherFromPair = claimedFromPair.Single() == first.Id ? second.Id : first.Id;
        (await store.GetRunAsync(otherFromPair, CancellationToken.None))!.Phase.Should().Be(JobPhase.Pending);
    }

    [Fact]
    public async Task KeyOrdered_claim_blocks_a_later_run_until_the_head_is_terminal()
    {
        if (!Enabled) return;
        var store = _store!;
        var ordered = new DeliveryTarget(
            ExecutionDeliveryProfile.Pull,
            "default",
            null,
            "default",
            ExecutionOrderingMode.KeyOrdered);
        const string key = "order:postgres:42";
        var first = (await store.SubmitAsync(
            NewSubmission("ordered-first", concurrencyKey: key, deliveryTarget: ordered),
            CancellationToken.None)).Run;
        var second = (await store.SubmitAsync(
            NewSubmission("ordered-second", concurrencyKey: key, deliveryTarget: ordered),
            CancellationToken.None)).Run;
        var worker = await RegisterAsync(store, "ordered-worker", "ordered-session", 2);

        var firstClaim = await store.ClaimAsync(
            Claim(worker) with { RunIds = new[] { first.Id } },
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None);
        var blocked = await store.ClaimAsync(
            Claim(worker) with { RunIds = new[] { second.Id } },
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None);

        firstClaim.Should().ContainSingle();
        blocked.Should().BeEmpty();
        (await store.CompleteAsync(
            Completion(worker, firstClaim.Single(), JobAttemptOutcome.Succeeded),
            TestRetryPolicy,
            CancellationToken.None)).Accepted.Should().BeTrue();

        var secondClaim = await store.ClaimAsync(
            Claim(worker) with { RunIds = new[] { second.Id } },
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None);
        secondClaim.Should().ContainSingle(job => job.RunId == second.Id);
    }

    [Fact]
    public async Task KeyOrdered_runs_on_different_execution_lanes_do_not_block_each_other()
    {
        if (!Enabled) return;
        var store = _store!;
        const string key = "order:postgres:lanes";
        var laneA = new DeliveryTarget(
            ExecutionDeliveryProfile.Pull,
            "lane-a",
            null,
            "default",
            ExecutionOrderingMode.KeyOrdered);
        var laneB = laneA with { ExecutionLane = "lane-b" };
        var first = (await store.SubmitAsync(
            NewSubmission("ordered-lane-a", concurrencyKey: key, deliveryTarget: laneA),
            CancellationToken.None)).Run;
        var second = (await store.SubmitAsync(
            NewSubmission("ordered-lane-b", concurrencyKey: key, deliveryTarget: laneB),
            CancellationToken.None)).Run;

        var workerA = await RegisterAsync(store, "ordered-lane-worker-a", "ordered-lane-session-a", executionLane: "lane-a");
        var workerB = await RegisterAsync(store, "ordered-lane-worker-b", "ordered-lane-session-b", executionLane: "lane-b");
        var firstClaim = await store.ClaimAsync(
            Claim(workerA) with { RunIds = new[] { first.Id } },
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None);
        var secondClaim = await store.ClaimAsync(
            Claim(workerB) with { RunIds = new[] { second.Id } },
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None);

        firstClaim.Should().ContainSingle(job => job.RunId == first.Id);
        secondClaim.Should().ContainSingle(job => job.RunId == second.Id);
    }

    [Fact]
    public async Task Batch_claim_excludes_a_pending_run_whose_concurrency_key_is_already_running()
    {
        if (!Enabled) return;
        var store = _store!;
        const string concurrencyKey = "already-running-concurrency-key";
        var running = (await store.SubmitAsync(
            NewSubmission("already-running-1", concurrencyKey: concurrencyKey),
            CancellationToken.None)).Run;
        var pendingSibling = (await store.SubmitAsync(
            NewSubmission("already-running-2", concurrencyKey: concurrencyKey),
            CancellationToken.None)).Run;
        var unrelated = (await store.SubmitAsync(
            NewSubmission("already-running-unrelated"),
            CancellationToken.None)).Run;

        var soloWorker = await RegisterAsync(store, "already-running-solo-worker", "already-running-solo-session", 1);
        var soloClaim = await store.ClaimAsync(
            Claim(soloWorker) with { RunIds = new[] { running.Id } },
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None);
        soloClaim.Should().ContainSingle();

        var batchWorker = await RegisterAsync(store, "already-running-batch-worker", "already-running-batch-session", 2);
        var batchClaim = await store.ClaimAsync(
            new ClaimJobsRequest(
                batchWorker.WorkerId,
                batchWorker.SessionId,
                batchWorker.Epoch,
                2,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            2,
            CancellationToken.None);

        batchClaim.Should().ContainSingle(job => job.RunId == unrelated.Id);
        (await store.GetRunAsync(pendingSibling.Id, CancellationToken.None))!.Phase.Should().Be(JobPhase.Pending);
    }

    [Fact]
    public async Task Batch_claim_excludes_a_run_that_loses_its_update_race_without_leaving_an_orphaned_attempt()
    {
        if (!Enabled) return;
        var store = _store!;
        var contested = (await store.SubmitAsync(
            NewSubmission("race-loss-contested"),
            CancellationToken.None)).Run;
        var sibling = (await store.SubmitAsync(
            NewSubmission("race-loss-sibling"),
            CancellationToken.None)).Run;

        var soloWorker = await RegisterAsync(store, "race-solo-worker", "race-solo-session", 1);
        var soloClaim = await store.ClaimAsync(
            Claim(soloWorker) with { RunIds = new[] { contested.Id } },
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None);
        soloClaim.Should().ContainSingle();

        var batchWorker = await RegisterAsync(store, "race-batch-worker", "race-batch-session", 2);
        var batchClaim = await store.ClaimAsync(
            new ClaimJobsRequest(
                batchWorker.WorkerId,
                batchWorker.SessionId,
                batchWorker.Epoch,
                2,
                new[] { "default" },
                new[] { "mail.send" }),
            TimeSpan.FromMinutes(1),
            2,
            CancellationToken.None);

        batchClaim.Should().ContainSingle(job => job.RunId == sibling.Id);
        var contestedAttempts = await store.GetAttemptsAsync(contested.Id, CancellationToken.None);
        contestedAttempts.Should().ContainSingle();
        (await store.GetRunAsync(contested.Id, CancellationToken.None))!
            .CurrentWorkerId.Should().Be(soloWorker.WorkerId);
    }

    private static DeliveryTarget BrokerDispatchTarget() =>
        new(ExecutionDeliveryProfile.BrokerDispatch, "default", "rabbitmq", "default");

    private static SubmitJobCommand NewSubmission(
        string? idempotencyKey = null,
        string queue = "default",
        DateTimeOffset? availableAt = null,
        int maxAttempts = 3,
        int priority = 0,
        string? concurrencyKey = null,
        DeliveryTarget? deliveryTarget = null) => new(
        "mail.send",
        "{\"to\":\"user@example.com\"}",
        queue,
        priority,
        availableAt ?? DateTimeOffset.UtcNow,
        idempotencyKey,
        concurrencyKey,
        maxAttempts,
        60,
        DeliveryTarget: deliveryTarget);

    private static JobScheduleRecord NewSchedule(DateTimeOffset nextFireAt) => new()
    {
        Id = "postgres-schedule",
        JobKey = "mail.send",
        PayloadJson = "{\"to\":\"scheduled@example.com\"}",
        CronExpression = "*/5 * * * *",
        TimeZoneId = "UTC",
        Queue = "default",
        TransportId = "rabbitmq",
        MisfirePolicy = MisfirePolicy.FireOnce,
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.Allow,
        MaxAttempts = 3,
        TimeoutSeconds = 60,
        ConcurrencyKey = "schedule:postgres:42",
        RetryPolicy = new RetryPolicy(
            BackoffStrategy.Fixed,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1)),
        Continuation = new Continuation
        {
            JobKey = "mail.scheduled.followup",
            PayloadJson = "{}"
        },
        Compensation = new Compensation
        {
            JobKey = "mail.scheduled.compensate",
            PayloadJson = "{}"
        },
        Enabled = true,
        NextFireAt = nextFireAt,
        CreatedAt = DateTimeOffset.UtcNow,
        UpdatedAt = DateTimeOffset.UtcNow
    };

    private static ValueTask<WorkerSessionRecord> RegisterAsync(
        PostgreSqlJobRuntimeStore store,
        string workerId,
        string sessionId,
        int maxConcurrency = 1,
        IReadOnlyList<string>? capabilities = null,
        string consumerGroup = "default",
        string executionLane = "default") => store.RegisterAsync(
        new RegisterWorkerSessionRequest(
            workerId,
            sessionId,
            "integration-test",
            "localhost",
            maxConcurrency,
            new[] { "default" },
            capabilities ?? new[] { "mail.send" },
            new Dictionary<string, string>(),
            consumerGroup,
            executionLane),
        CancellationToken.None);

    [Fact]
    public async Task Maintenance_removes_published_outbox_and_unkeyed_terminal_runs()
    {
        if (!Enabled) return;
        var store = _store!;
        var unkeyedRun = (await store.SubmitAsync(
            NewSubmission(deliveryTarget: BrokerDispatchTarget()),
            CancellationToken.None)).Run;
        var keyedRun = (await store.SubmitAsync(
            NewSubmission(idempotencyKey: "maintenance-retain-me"),
            CancellationToken.None)).Run;

        var worker = await RegisterAsync(store, "maintenance-worker", "maintenance-session");
        var claim = (await store.ClaimAsync(
            Claim(worker) with { RunIds = new[] { unkeyedRun.Id } },
            TimeSpan.FromMinutes(1),
            1,
            CancellationToken.None)).Single();
        await store.CompleteAsync(
            Completion(worker, claim, JobAttemptOutcome.Succeeded),
            TestRetryPolicy,
            CancellationToken.None);
        await store.DispatchOnceAsync(
            TimeSpan.FromMinutes(1),
            TimeSpan.Zero,
            10,
            (_, _) => ValueTask.CompletedTask,
            CancellationToken.None);

        var outboxDeleted = await store.DeletePublishedOutboxAsync(
            DateTimeOffset.UtcNow.AddMinutes(1),
            100,
            CancellationToken.None);
        var terminalDeleted = await store.DeleteUnkeyedTerminalRunsAsync(
            DateTimeOffset.UtcNow.AddMinutes(1),
            100,
            CancellationToken.None);

        outboxDeleted.Should().BeGreaterThan(0);
        terminalDeleted.Should().Be(1);
        (await store.GetRunAsync(unkeyedRun.Id, CancellationToken.None)).Should().BeNull();
        (await store.GetRunAsync(keyedRun.Id, CancellationToken.None)).Should().NotBeNull();
    }

    private static ClaimJobsRequest Claim(WorkerSessionRecord session) => new(
        session.WorkerId,
        session.SessionId,
        session.Epoch,
        1,
        new[] { "default" },
        new[] { "mail.send" },
        ConsumerGroup: session.ConsumerGroup,
        ExecutionLane: session.ExecutionLane);

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
