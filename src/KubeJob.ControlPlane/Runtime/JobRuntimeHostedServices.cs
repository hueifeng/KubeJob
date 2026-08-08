using KubeJob.ControlPlane.Telemetry;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.ControlPlane.Runtime;

/// <summary>
/// Default wake notifier for PostgresManaged deployments. PostgreSQL remains
/// the execution authority; adapters may replace this with a best-effort wake
/// signal without carrying execution ownership.
/// </summary>
public sealed class NoopWorkAvailableNotifier : IWorkAvailableNotifier
{
    public ValueTask PublishAsync(
        WorkAvailableSignal signal,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Publishes PostgresManaged work-available wake signals from the transactional
/// outbox. BrokerNative jobs bypass this service entirely and publish directly
/// through IMessageTransportPublisher.
/// </summary>
public sealed class OutboxPublisherService : BackgroundService
{
    private readonly IOutboxStore _store;
    private readonly IWorkAvailableNotifier _notifier;
    private readonly OutboxPublisherSignal _wake;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<OutboxPublisherService> _logger;
    private readonly KubeJobControlPlaneMetrics? _metrics;

    public OutboxPublisherService(
        IOutboxStore store,
        IWorkAvailableNotifier notifier,
        OutboxPublisherSignal wake,
        IOptions<JobRuntimeOptions> options,
        ILogger<OutboxPublisherService> logger,
        KubeJobControlPlaneMetrics? metrics = null)
    {
        _store = store;
        _notifier = notifier;
        _wake = wake;
        _options = options.Value;
        _logger = logger;
        _metrics = metrics;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _options.Validate();

        while (!stoppingToken.IsCancellationRequested)
        {
            var dispatchedAny = false;
            try
            {
                var result = await DispatchParallelAsync(stoppingToken);
                dispatchedAny = result.DispatchedIds.Count > 0;

                if (result.FailedIds.Count > 0)
                {
                    _logger.LogWarning(
                        "Failed to publish {Count} KubeJob outbox message(s); state store has already marked them Failed for retry",
                        result.FailedIds.Count);
                }

                if (result.Abandoned.Count > 0)
                {
                    _logger.LogError(
                        "Abandoned {Count} KubeJob outbox message(s) because their event type is permanently invalid",
                        result.Abandoned.Count);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KubeJob outbox publisher iteration failed");
                await Task.Delay(_options.OutboxFailureDelay, stoppingToken);
            }

            if (!dispatchedAny)
            {
                try
                {
                    await WaitForWakeOrTimeoutAsync(_options.OutboxPollInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private async Task WaitForWakeOrTimeoutAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var delay = Task.Delay(timeout, cancellationToken);
        var wake = _wake.Reader.WaitToReadAsync(cancellationToken).AsTask();
        var first = await Task.WhenAny(wake, delay).ConfigureAwait(false);

        if (first == wake && wake.IsCompletedSuccessfully && wake.Result)
        {
            while (_wake.Reader.TryRead(out _))
            {
            }
        }
    }

    private async Task<OutboxDispatchBatch> DispatchParallelAsync(
        CancellationToken cancellationToken)
    {
        var workerCount = Math.Clamp(_options.OutboxPublishConcurrency, 1, 32);
        var perWorkerBatch = Math.Max(1, _options.OutboxBatchSize / workerCount);

        var workers = Enumerable.Range(0, workerCount)
            .Select(_ => Task.Run(
                () => DispatchWorkerLoopAsync(perWorkerBatch, cancellationToken),
                cancellationToken))
            .ToArray();

        OutboxDispatchBatch[] results;
        try
        {
            results = await Task.WhenAll(workers);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            results = Array.Empty<OutboxDispatchBatch>();
        }

        return new OutboxDispatchBatch(
            results.SelectMany(r => r.DispatchedIds).ToArray(),
            results.SelectMany(r => r.FailedIds).ToArray(),
            results.SelectMany(r => r.Abandoned).ToArray());
    }

    private async Task<OutboxDispatchBatch> DispatchWorkerLoopAsync(
        int batchSize,
        CancellationToken cancellationToken)
    {
        var totalDispatched = new List<string>(batchSize);
        var totalFailed = new List<string>(batchSize);
        var totalAbandoned = new List<string>(batchSize);

        while (!cancellationToken.IsCancellationRequested)
        {
            OutboxDispatchBatch batch;
            try
            {
                batch = await _store.DispatchOnceAsync(
                    _options.OutboxClaimDuration,
                    _options.OutboxFailureDelay,
                    batchSize,
                    DispatchOneAsync,
                    cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            totalDispatched.AddRange(batch.DispatchedIds);
            totalFailed.AddRange(batch.FailedIds);
            totalAbandoned.AddRange(batch.Abandoned);

            if (batch.DispatchedIds.Count == 0
                && batch.FailedIds.Count == 0
                && batch.Abandoned.Count == 0)
            {
                break;
            }
        }

        return new OutboxDispatchBatch(totalDispatched, totalFailed, totalAbandoned);
    }

    private async ValueTask DispatchOneAsync(
        OutboxMessageRecord message,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(message.EventType, OutboxEventTypes.WorkAvailable, StringComparison.Ordinal))
        {
            _logger.LogWarning(
                "KubeJob outbox row {MessageId} has unsupported EventType {EventType}; marking Abandoned",
                message.Id,
                message.EventType);
            throw new PermanentOutboxException(
                $"Unsupported managed outbox event type '{message.EventType}'.");
        }

        // PostgresManaged is the sole authority for durable Runs. The outbox
        // only emits an optional wake signal; workers still claim from PG.
        var signal = WorkAvailableSignal.FromOutbox(message);
        await _notifier.PublishAsync(signal, cancellationToken);

        if (_metrics?.IsOutboxPublishLagEnabled == true)
        {
            _metrics.OutboxPublished(DateTimeOffset.UtcNow - message.AvailableAt);
        }
    }
}

public sealed class RuntimeRetentionService : BackgroundService
{
    private readonly IJobRuntimeMaintenanceStore _store;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<RuntimeRetentionService> _logger;

    public RuntimeRetentionService(
        IJobRuntimeMaintenanceStore store,
        IOptions<JobRuntimeOptions> options,
        ILogger<RuntimeRetentionService> logger)
    {
        _store = store;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _options.Validate();
        using var timer = new PeriodicTimer(_options.RetentionPollInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    var now = DateTimeOffset.UtcNow;
                    var outboxDeleted = await _store.DeletePublishedOutboxAsync(
                        now - _options.PublishedOutboxRetention,
                        _options.RetentionBatchSize,
                        stoppingToken);
                    var terminalDeleted = await _store.DeleteUnkeyedTerminalRunsAsync(
                        now - _options.UnkeyedTerminalRetention,
                        _options.RetentionBatchSize,
                        stoppingToken);

                    if (outboxDeleted > 0 || terminalDeleted > 0)
                    {
                        _logger.LogInformation(
                            "KubeJob retention removed {OutboxCount} published outbox row(s) and {TerminalCount} unkeyed terminal run(s)",
                            outboxDeleted,
                            terminalDeleted);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "KubeJob retention iteration failed");
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}

public sealed class LeaseReaperService : BackgroundService
{
    private readonly IJobCompletionStore _store;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<LeaseReaperService> _logger;
    private readonly KubeJobControlPlaneMetrics? _metrics;

    public LeaseReaperService(
        IJobCompletionStore store,
        IOptions<JobRuntimeOptions> options,
        ILogger<LeaseReaperService> logger,
        KubeJobControlPlaneMetrics? metrics = null)
    {
        _store = store;
        _options = options.Value;
        _logger = logger;
        _metrics = metrics;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _options.Validate();

        using var timer = new PeriodicTimer(_options.LeaseReaperInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    var count = await _store.RequeueExpiredLeasesAsync(
                        DateTimeOffset.UtcNow,
                        _options.RetryPolicy,
                        _options.LeaseReaperBatchSize,
                        stoppingToken);

                    if (count > 0)
                    {
                        _logger.LogWarning("Reconciled {Count} expired KubeJob attempt leases", count);
                    }

                    _metrics?.LeasesReclaimed(count);
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
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}
