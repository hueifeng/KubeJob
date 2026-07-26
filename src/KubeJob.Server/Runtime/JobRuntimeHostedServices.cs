using System.Text.Json;
using KubeJob.Core.Runtime;
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
        WorkAvailableSignal signal,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Publishes a per-group cancel signal for Direct Dispatch Mode. MQ transport
/// packages replace the default no-op with a fanout publisher; the outbox
/// publisher calls into this interface instead of an MQ client so the
/// transport seam is consistent with <see cref="IWorkAvailableNotifier"/> and
/// <see cref="IExecutionDispatcher"/>.
/// </summary>
public interface ICancelPublisher
{
    ValueTask PublishAsync(string group, string runId, CancellationToken cancellationToken);
}

/// <summary>
/// Default no-op cancel publisher for Pull deployments. The control plane
/// still records <c>cancel</c> outbox rows so log analysis can prove cancel
/// events flowed; the broker never sees them, and lease reaper drives
/// cancellation as before.
/// </summary>
public sealed class NoopCancelPublisher : ICancelPublisher
{
    public ValueTask PublishAsync(string group, string runId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }
}

public sealed class OutboxPublisherService : BackgroundService
{
    private readonly IOutboxStore _store;
    private readonly IWorkAvailableNotifier _notifier;
    private readonly IQueueRouter _queueRouter;
    private readonly IExecutionDispatcher _dispatcher;
    private readonly ICancelPublisher _cancelPublisher;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<OutboxPublisherService> _logger;

    public OutboxPublisherService(
        IOutboxStore store,
        IWorkAvailableNotifier notifier,
        IQueueRouter queueRouter,
        IExecutionDispatcher dispatcher,
        ICancelPublisher cancelPublisher,
        IOptions<JobRuntimeOptions> options,
        ILogger<OutboxPublisherService> logger)
    {
        _store = store;
        _notifier = notifier;
        _queueRouter = queueRouter;
        _dispatcher = dispatcher;
        _cancelPublisher = cancelPublisher;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _options.Validate();

        while (!stoppingToken.IsCancellationRequested)
        {
            var dispatchedAny = false;
            try
            {
                var results = await DispatchParallelAsync(stoppingToken);
                var result = new OutboxDispatchBatch(
                    results.SelectMany(x => x.DispatchedIds).ToArray(),
                    results.SelectMany(x => x.FailedIds).ToArray(),
                    results.SelectMany(x => x.Abandoned).ToArray());

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
                        "Abandoned {Count} KubeJob outbox message(s) because their event type or routing is permanently invalid",
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
                    await Task.Delay(_options.OutboxPollInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private Task<OutboxDispatchBatch[]> DispatchParallelAsync(
        CancellationToken cancellationToken)
    {
        var workerCount = Math.Min(_options.OutboxBatchSize, _options.OutboxPublishConcurrency);
        return Task.WhenAll(Enumerable.Range(0, workerCount).Select(_ =>
            _store.DispatchOnceAsync(
                _options.OutboxClaimDuration,
                _options.OutboxFailureDelay,
                1,
                DispatchOneAsync,
                cancellationToken).AsTask()));
    }

    private async ValueTask DispatchOneAsync(
        OutboxMessageRecord message,
        CancellationToken cancellationToken)
    {
        switch (message.EventType)
        {
            case OutboxEventTypes.WorkAvailable:
                await DispatchWorkAvailableAsync(message, cancellationToken);
                break;
            case OutboxEventTypes.Cancel:
                await DispatchCancelAsync(message, cancellationToken);
                break;
            default:
                _logger.LogWarning(
                    "KubeJob outbox row {MessageId} has unknown EventType {EventType}; marking Abandoned",
                    message.Id,
                    message.EventType);
                throw new PermanentOutboxException(
                    $"Unsupported outbox event type '{message.EventType}'.");
        }
    }

    private async ValueTask DispatchWorkAvailableAsync(
        OutboxMessageRecord message,
        CancellationToken cancellationToken)
    {
        var signal = WorkAvailableSignal.FromOutbox(message);
        var route = _queueRouter.Resolve(message.Queue);
        switch (route.Profile)
        {
            case ExecutionDeliveryProfile.Pull:
                await _notifier.PublishAsync(signal, cancellationToken);
                break;
            case ExecutionDeliveryProfile.BrokerDispatch:
                await _dispatcher.DispatchAsync(
                    ExecutionEnvelope.FromWorkAvailableSignal(signal),
                    cancellationToken);
                break;
            default:
                throw new InvalidOperationException(
                    $"Unsupported execution delivery profile '{route.Profile}'.");
        }
    }

    private async ValueTask DispatchCancelAsync(
        OutboxMessageRecord message,
        CancellationToken cancellationToken)
    {
        // For cancel rows, message.Queue carries the worker group identifier
        // (the cancel exchange is per-group, not per logical queue).
        var group = message.Queue;
        if (string.IsNullOrWhiteSpace(group))
        {
            throw new InvalidOperationException(
                $"KubeJob cancel outbox row {message.Id} is missing the group identifier in the Queue column.");
        }

        RunIdPayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize<RunIdPayload>(
                message.PayloadJson,
                new JsonSerializerOptions(JsonSerializerDefaults.Web));
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException(
                $"KubeJob cancel outbox row {message.Id} has an unparseable payload: {ex.Message}",
                ex);
        }

        if (payload is null || string.IsNullOrWhiteSpace(payload.RunId))
        {
            throw new InvalidOperationException(
                $"KubeJob cancel outbox row {message.Id} is missing a runId.");
        }

        await _cancelPublisher.PublishAsync(group, payload.RunId, cancellationToken);
    }

    private sealed record RunIdPayload(string RunId);
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
        try
        {
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
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}
