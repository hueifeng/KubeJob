using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Default notifier for database-polling deployments. MQ packages replace this service.
/// </summary>
public sealed class PollingWorkAvailableNotifier : IWorkAvailableNotifier
{
    public ValueTask PublishAsync(
        string queue,
        string payloadJson,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }
}

public sealed class OutboxPublisherService : BackgroundService
{
    private readonly IOutboxStore _store;
    private readonly IWorkAvailableNotifier _notifier;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<OutboxPublisherService> _logger;

    public OutboxPublisherService(
        IOutboxStore store,
        IWorkAvailableNotifier notifier,
        IOptions<JobRuntimeOptions> options,
        ILogger<OutboxPublisherService> logger)
    {
        _store = store;
        _notifier = notifier;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _options.Validate();

        while (!stoppingToken.IsCancellationRequested)
        {
            var publishedAny = false;
            try
            {
                var now = DateTimeOffset.UtcNow;
                var messages = await _store.ClaimPendingAsync(
                    now,
                    _options.OutboxBatchSize,
                    stoppingToken);

                foreach (var message in messages)
                {
                    try
                    {
                        await _notifier.PublishAsync(
                            message.Queue,
                            message.PayloadJson,
                            stoppingToken);
                        await _store.MarkPublishedAsync(
                            message.Id,
                            DateTimeOffset.UtcNow,
                            stoppingToken);
                        publishedAny = true;
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(
                            ex,
                            "Failed to publish KubeJob outbox message {MessageId} for queue {Queue}",
                            message.Id,
                            message.Queue);

                        await _store.MarkFailedAsync(
                            message.Id,
                            ex.Message,
                            DateTimeOffset.UtcNow.Add(_options.OutboxFailureDelay),
                            stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KubeJob outbox publisher iteration failed");
            }

            if (!publishedAny)
            {
                await Task.Delay(_options.OutboxPollInterval, stoppingToken);
            }
        }
    }
}

public sealed class LeaseReaperService : BackgroundService
{
    private readonly IJobCompletionStore _store;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<LeaseReaperService> _logger;

    public LeaseReaperService(
        IJobCompletionStore store,
        IOptions<JobRuntimeOptions> options,
        ILogger<LeaseReaperService> logger)
    {
        _store = store;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _options.Validate();

        using var timer = new PeriodicTimer(_options.LeaseReaperInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                var count = await _store.RequeueExpiredLeasesAsync(
                    DateTimeOffset.UtcNow,
                    _options.RetryDelay,
                    _options.LeaseReaperBatchSize,
                    stoppingToken);

                if (count > 0)
                {
                    _logger.LogWarning("Reconciled {Count} expired KubeJob attempt leases", count);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KubeJob lease reconciliation iteration failed");
            }
        }
    }
}
