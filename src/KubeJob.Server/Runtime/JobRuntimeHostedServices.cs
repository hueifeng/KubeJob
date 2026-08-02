using System.Text.Json;
using KubeJob.ControlPlane.Telemetry;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Default notifier for database-polling deployments. MQ packages replace
/// this service via <see cref="KubeJob.Server.Runtime.IWorkAvailableNotifier"/>.
/// The no-op name reflects the fact that polling workers discover claimable
/// Runs through the control plane, not via a wake signal.
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
    private readonly IExecutionTransportRegistry _transports;
    private readonly ICancelPublisher _cancelPublisher;
    private readonly OutboxPublisherSignal _wake;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<OutboxPublisherService> _logger;
    private readonly KubeJobControlPlaneMetrics? _metrics;

    public OutboxPublisherService(
        IOutboxStore store,
        IWorkAvailableNotifier notifier,
        IExecutionTransportRegistry transports,
        ICancelPublisher cancelPublisher,
        OutboxPublisherSignal wake,
        IOptions<JobRuntimeOptions> options,
        ILogger<OutboxPublisherService> logger,
        KubeJobControlPlaneMetrics? metrics = null)
    {
        _store = store;
        _notifier = notifier;
        _transports = transports;
        _cancelPublisher = cancelPublisher;
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
                    await WaitForWakeOrTimeoutAsync(_options.OutboxPollInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Idles until either an in-process writer signals a new outbox row
    /// (<see cref="OutboxPublisherSignal"/>) or the safety-net poll interval
    /// elapses — whichever comes first. Drains the signal channel so the
    /// next iteration's empty-scan result still respects the poll cadence.
    /// </summary>
    private async Task WaitForWakeOrTimeoutAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var delay = Task.Delay(timeout, cancellationToken);
        var wake = _wake.Reader.WaitToReadAsync(cancellationToken).AsTask();
        var first = await Task.WhenAny(wake, delay).ConfigureAwait(false);

        if (first == wake && wake.IsCompletedSuccessfully && wake.Result)
        {
            // Drain the coalesced signal so a single wake produces one scan,
            // even if Signal() was called many times during the wait.
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

        var dispatchedIds = results.SelectMany(r => r.DispatchedIds).ToArray();
        var failedIds = results.SelectMany(r => r.FailedIds).ToArray();
        var abandonedIds = results.SelectMany(r => r.Abandoned).ToArray();
        return new OutboxDispatchBatch(dispatchedIds, failedIds, abandonedIds);
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

            if (batch.DispatchedIds.Count > 0)
            {
                totalDispatched.AddRange(batch.DispatchedIds);
            }

            if (batch.FailedIds.Count > 0)
            {
                totalFailed.AddRange(batch.FailedIds);
            }

            if (batch.Abandoned.Count > 0)
            {
                totalAbandoned.AddRange(batch.Abandoned);
            }

            // The store drained the queue for this iteration; yield so other
            // workers can take a turn before the outer poll cadence kicks in.
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
        switch (message.DeliveryProfile)
        {
            case ExecutionDeliveryProfile.Pull:
                await _notifier.PublishAsync(signal, cancellationToken);
                break;
            case ExecutionDeliveryProfile.BrokerDispatch:
                var transport = _transports.Resolve(message.TransportId
                    ?? throw new InvalidOperationException(
                        $"KubeJob outbox row {message.Id} is missing a transport ID."));
                await transport.PublishAsync(
                    ExecutionEnvelope.FromWorkAvailableSignal(signal),
                    cancellationToken);
                break;
            default:
                throw new InvalidOperationException(
                    $"Unsupported execution delivery profile '{message.DeliveryProfile}'.");
        }

        if (_metrics?.IsOutboxPublishLagEnabled == true)
        {
            _metrics.OutboxPublished(DateTimeOffset.UtcNow - message.AvailableAt);
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
