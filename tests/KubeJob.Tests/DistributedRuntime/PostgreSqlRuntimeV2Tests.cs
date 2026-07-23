using Dapper;
using FluentAssertions;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit.Sdk;

namespace KubeJob.Tests.DistributedRuntime;

/// <summary>
/// Real PostgreSQL fencing smoke tests. They are opt-in because they require a local
/// container runtime; CI should set KUBEJOB_RUN_POSTGRES_TESTS=1 on the integration job.
/// </summary>
public sealed class PostgreSqlRuntimeV2Tests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("kubejob")
        .WithUsername("kubejob")
        .WithPassword("kubejob")
        .Build();

    private string ConnectionString => _postgres.GetConnectionString();

    public async Task InitializeAsync()
    {
        if (Environment.GetEnvironmentVariable("KUBEJOB_RUN_POSTGRES_TESTS") != "1") return;
        await _postgres.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (Environment.GetEnvironmentVariable("KUBEJOB_RUN_POSTGRES_TESTS") == "1")
            await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task Concurrent_claimers_only_transition_a_run_once()
    {
        SkipUnlessEnabled();
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        var table = "runs_" + Guid.NewGuid().ToString("N");
        await connection.ExecuteAsync($"CREATE TABLE {table}(id int primary key, status int not null, token bigint not null default 0)");
        await connection.ExecuteAsync($"INSERT INTO {table}(id,status) VALUES (1,0)");

        async Task<bool> ClaimAsync(long token)
        {
            await using var claimant = new NpgsqlConnection(ConnectionString);
            await claimant.OpenAsync();
            await using var tx = await claimant.BeginTransactionAsync();
            var changed = await claimant.ExecuteAsync(new CommandDefinition(
                $"UPDATE {table} SET status=2, token=@token WHERE id=(SELECT id FROM {table} WHERE status=0 FOR UPDATE SKIP LOCKED LIMIT 1) AND status=0",
                new { token }, tx));
            await tx.CommitAsync();
            return changed == 1;
        }

        var results = await Task.WhenAll(ClaimAsync(101), ClaimAsync(202));
        results.Count(static x => x).Should().Be(1);
        (await connection.ExecuteScalarAsync<long>($"SELECT token FROM {table} WHERE id=1")).Should().BeOneOf(101, 202);
    }

    [Fact]
    public async Task Stale_attempt_cannot_complete_after_fencing()
    {
        SkipUnlessEnabled();
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        var table = "attempts_" + Guid.NewGuid().ToString("N");
        await connection.ExecuteAsync($"CREATE TABLE {table}(run_id int primary key, status int not null, token bigint not null)");
        await connection.ExecuteAsync($"INSERT INTO {table} VALUES (1,2,200)");
        var affected = await connection.ExecuteAsync(
            $"UPDATE {table} SET status=3 WHERE run_id=1 AND status=2 AND token=@token", new { token = 100L });
        affected.Should().Be(0);
        (await connection.ExecuteScalarAsync<int>($"SELECT status FROM {table} WHERE run_id=1")).Should().Be(2);
    }

    private static void SkipUnlessEnabled()
    {
        if (Environment.GetEnvironmentVariable("KUBEJOB_RUN_POSTGRES_TESTS") != "1")
            throw new SkipException();
    }
}
