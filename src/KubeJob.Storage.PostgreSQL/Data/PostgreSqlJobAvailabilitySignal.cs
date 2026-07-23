using KubeJob.Server.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Data;

public sealed partial class PostgreSqlJobAvailabilitySignal : BackgroundService, IJobAvailabilitySignal
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly ILogger<PostgreSqlJobAvailabilitySignal> _logger;
    private TaskCompletionSource<bool> _pulse = NewPulse();
    private long _version;

    public PostgreSqlJobAvailabilitySignal(NpgsqlDataSource dataSource,
        ILogger<PostgreSqlJobAvailabilitySignal> logger)
    {
        _dataSource = dataSource;
        _logger = logger;
    }

    public long Version => Volatile.Read(ref _version);

    public async ValueTask WaitForChangeAsync(long observedVersion, TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        if (Version != observedVersion) return;
        var task = Volatile.Read(ref _pulse).Task;
        if (Version != observedVersion) return;
        try { await task.WaitAsync(timeout, cancellationToken); }
        catch (TimeoutException) { }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var connection = await _dataSource.OpenConnectionAsync(stoppingToken);
                connection.Notification += OnNotification;
                await using var command = new NpgsqlCommand("LISTEN kubejob_runs", connection);
                await command.ExecuteNonQueryAsync(stoppingToken);
                Connected(_logger);
                while (!stoppingToken.IsCancellationRequested) await connection.WaitAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                Disconnected(_logger, ex);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }

    private void OnNotification(object sender, NpgsqlNotificationEventArgs args)
    {
        Interlocked.Increment(ref _version);
        Interlocked.Exchange(ref _pulse, NewPulse()).TrySetResult(true);
    }

    private static TaskCompletionSource<bool> NewPulse() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    [LoggerMessage(2401, LogLevel.Debug, "PostgreSQL KubeJob availability listener connected.")]
    private static partial void Connected(ILogger logger);
    [LoggerMessage(2402, LogLevel.Warning, "PostgreSQL KubeJob availability listener disconnected; retrying.")]
    private static partial void Disconnected(ILogger logger, Exception exception);
}
