using System.Collections.Concurrent;
using System.Threading.Channels;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Runtime;

/// <summary>
/// Bounded pull worker using an abstract control-plane transport. The same
/// execution engine is used for remote HTTP and unified in-process hosting.
/// </summary>
public sealed class WorkerRuntimeService : BackgroundService
{
    private const string TruncatedSuffix = "\n...[truncated]";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly JobHandlerRegistry _registry;
    private readonly IWorkerRuntimeClient _runtimeClient;
    private readonly IWorkerClaimTrigger _claimTrigger;
    private readonly KubeJobWorkerOptions _options;
    private readonly ILogger<WorkerRuntimeService> _logger;
    private Channel<ClaimedJob> _channel;
    private readonly ConcurrentDictionary<string, OwnedAttempt> _owned = new(StringComparer.Ordinal);
    private string _sessionId = Guid.NewGuid().ToString("N");
    private readonly string _hostName = Environment.MachineName;
    private TaskCompletionSource<WorkerSessionContext> _sessionReady = CreateSessionReadySource();

    private CancellationTokenSource? _sessionLifetime;
    private long _sessionEpoch;
    private int _reservedSlots;
    private int _draining;

    public WorkerRuntimeService(
        IServiceScopeFactory scopeFactory,
        JobHandlerRegistry registry,
        IWorkerRuntimeClient runtimeClient,
        IWorkerClaimTrigger claimTrigger,
        IOptions<KubeJobWorkerOptions> options,
        ILogger<WorkerRuntimeService> logger)
    {
        _scopeFactory = scopeFactory;
        _registry = registry;
        _runtimeClient = runtimeClient;
        _claimTrigger = claimTrigger;
        _options = options.Value;
        _logger = logger;
        _options.Validate();

        _channel = CreateExecutionChannel();
    }

    private static TaskCompletionSource<WorkerSessionContext> CreateSessionReadySource() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private Channel<ClaimedJob> CreateExecutionChannel() =>
        Channel.CreateBounded<ClaimedJob>(new BoundedChannelOptions(_options.MaxConcurrentJobs)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false,
            AllowSynchronousContinuations = false
        });

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_registry.Capabilities.Count == 0)
        {
            var exception = new InvalidOperationException(
                "The worker has no typed handlers. Register at least one AddKubeJobHandler<TJob, TPayload>.");
            _sessionReady.TrySetException(exception);
            throw exception;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            PrepareNextSession();
            try
            {
                await RunSessionAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KubeJob worker session loop failed; restarting session");
            }

            if (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private async Task RunSessionAsync(CancellationToken stoppingToken)
    {
        using var sessionLifetime = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _sessionLifetime = sessionLifetime;
        var runtimeToken = sessionLifetime.Token;

        try
        {
            await RegisterSessionUntilAcceptedAsync(runtimeToken);
            _sessionReady.TrySetResult(new WorkerSessionContext(
                _options.WorkerId,
                _sessionId,
                Volatile.Read(ref _sessionEpoch),
                _hostName,
                _options.BuildId));
            _logger.LogInformation(
                "KubeJob worker {WorkerId} session {SessionId}/{Epoch} started with capacity {Capacity}",
                _options.WorkerId,
                _sessionId,
                _sessionEpoch,
                _options.MaxConcurrentJobs);

            var consumers = Enumerable.Range(0, _options.MaxConcurrentJobs)
                .Select(index => ConsumeAsync(index, runtimeToken))
                .ToArray();
            var coordinationLoops = new[]
            {
                ClaimLoopAsync(runtimeToken),
                RenewLoopAsync(runtimeToken),
                HeartbeatLoopAsync(runtimeToken)
            };

            await Task.WhenAll(consumers.Concat(coordinationLoops));
        }
        finally
        {
            sessionLifetime.Cancel();
            _channel.Writer.TryComplete();
            if (!_sessionReady.Task.IsCompleted)
            {
                _sessionReady.TrySetCanceled();
            }
            Interlocked.CompareExchange(ref _sessionLifetime, null, sessionLifetime);
        }
    }

    private void PrepareNextSession()
    {
        foreach (var owned in _owned.Values)
        {
            TryCancelOwnedAttempt(owned);
        }

        Volatile.Write(ref _reservedSlots, 0);
        Volatile.Write(ref _sessionEpoch, 0);
        Interlocked.Exchange(ref _draining, 0);
        _sessionId = Guid.NewGuid().ToString("N");
        _sessionReady = CreateSessionReadySource();
        _channel = CreateExecutionChannel();
    }

    public string SessionId => _sessionId;

    public bool TryCancelRun(string runId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        var canceled = false;
        foreach (var owned in _owned.Values)
        {
            if (string.Equals(owned.Job.RunId, runId, StringComparison.Ordinal))
            {
                TryCancelOwnedAttempt(owned);
                canceled = true;
            }
        }

        return canceled;
    }

    /// <summary>
    /// Admits and executes one broker-delivered Run through the same bounded
    /// channel used by Pull claims. The caller may ACK only when the result is
    /// Completed; Retry means the broker delivery must remain/re-enter queued
    /// state, and Reject is a permanent delivery decision.
    /// </summary>
    public async ValueTask<ExecutionEnvelopeProcessingResult> ProcessExecutionEnvelopeAsync(
        ExecutionEnvelope envelope,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var session = await _sessionReady.Task.WaitAsync(cancellationToken);
        if (Volatile.Read(ref _draining) != 0)
        {
            // The worker is shutting down. The envelope will be redelivered to
            // another worker; we must not loop locally or the broker will keep
            // assigning this envelope to a dead session.
            return new ExecutionEnvelopeProcessingResult(
                ExecutionEnvelopeProcessingStatus.Retry,
                "worker_draining");
        }

        if (!_options.Queues.Contains(envelope.Queue, StringComparer.Ordinal))
        {
            return new ExecutionEnvelopeProcessingResult(
                ExecutionEnvelopeProcessingStatus.Retry,
                "worker_not_configured_for_queue");
        }

        var availableSlots = _options.MaxConcurrentJobs - Volatile.Read(ref _reservedSlots);
        if (availableSlots <= 0)
        {
            return new ExecutionEnvelopeProcessingResult(
                ExecutionEnvelopeProcessingStatus.Retry,
                "worker_capacity_exhausted");
        }

        var admission = await _runtimeClient.AdmitAsync(
            new AdmitExecutionRequest(
                session.WorkerId,
                session.SessionId,
                Volatile.Read(ref _sessionEpoch),
                availableSlots,
                envelope.RunId,
                _options.Queues,
                _registry.Capabilities),
            cancellationToken);

        switch (admission.Status)
        {
            case ExecutionAdmissionStatus.AlreadyTerminal:
                return new ExecutionEnvelopeProcessingResult(
                    ExecutionEnvelopeProcessingStatus.Completed,
                    admission.Reason);
            case ExecutionAdmissionStatus.NotFound:
            case ExecutionAdmissionStatus.Rejected:
                // Defensive: the control plane no longer returns Rejected for
                // recoverable routing mismatches — those come back as Retry.
                // Reaching this branch indicates a transport-level fault
                // (null Job on an Admitted response, an unexpected enum
                // value, etc.), so surface it as a Reject to drop the envelope
                // rather than spin in the broker's retry queue.
                return new ExecutionEnvelopeProcessingResult(
                    ExecutionEnvelopeProcessingStatus.Reject,
                    admission.Reason ?? "invalid_admission_response");
            case ExecutionAdmissionStatus.Retry:
                return new ExecutionEnvelopeProcessingResult(
                    ExecutionEnvelopeProcessingStatus.Retry,
                    admission.Reason);
            case ExecutionAdmissionStatus.Admitted when admission.Job is not null:
                try
                {
                    return await EnqueueAdmittedExecutionAsync(admission.Job, cancellationToken);
                }
                catch (OperationCanceledException) when (Volatile.Read(ref _draining) != 0)
                {
                    // Session was canceled mid-enqueue; the broker must not
                    // redeliver to this worker.
                    return new ExecutionEnvelopeProcessingResult(
                        ExecutionEnvelopeProcessingStatus.Retry,
                        "worker_session_canceled");
                }
            default:
                return new ExecutionEnvelopeProcessingResult(
                    ExecutionEnvelopeProcessingStatus.Reject,
                    "invalid_admission_response");
        }
    }

    private async ValueTask<ExecutionEnvelopeProcessingResult> EnqueueAdmittedExecutionAsync(
        ClaimedJob job,
        CancellationToken cancellationToken)
    {
        var owned = new OwnedAttempt(job, _sessionId);
        if (!_owned.TryAdd(job.AttemptId, owned))
        {
            owned.CancellationSource.Dispose();
            return new ExecutionEnvelopeProcessingResult(
                ExecutionEnvelopeProcessingStatus.Retry,
                "attempt_already_owned");
        }

        Interlocked.Increment(ref _reservedSlots);
        // Combine the session lifetime with the caller's token so a broker-side
        // cancellation or worker drain can unblock the write even when the
        // caller passed a token that hasn't observed the session fence yet.
        using var writeCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            owned.CancellationSource.Token);
        try
        {
            await _channel.Writer.WriteAsync(job, writeCts.Token);
            var completionReported = await owned.Completion.Task.WaitAsync(cancellationToken);
            return new ExecutionEnvelopeProcessingResult(
                completionReported
                    ? ExecutionEnvelopeProcessingStatus.Completed
                    : ExecutionEnvelopeProcessingStatus.Retry,
                completionReported ? null : "completion_not_durable");
        }
        catch (ChannelClosedException)
        {
            return new ExecutionEnvelopeProcessingResult(
                ExecutionEnvelopeProcessingStatus.Reject,
                "worker_execution_channel_closed");
        }
        finally
        {
            ReleaseOwnedAttempt(job.AttemptId);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        Interlocked.Exchange(ref _draining, 1);
        _channel.Writer.TryComplete();
        await SendHeartbeatBestEffortAsync(WorkerSessionState.Draining, cancellationToken);

        var deadline = DateTimeOffset.UtcNow.Add(_options.DrainTimeout);
        while (!_owned.IsEmpty
               && DateTimeOffset.UtcNow < deadline
               && !cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }

        if (!_owned.IsEmpty)
        {
            CancelOwnedAttempts();
        }

        await CloseSessionBestEffortAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }

    private async Task RegisterSessionUntilAcceptedAsync(CancellationToken cancellationToken)
    {
        var request = new RegisterWorkerSessionRequest(
            _options.WorkerId,
            _sessionId,
            _options.BuildId,
            _hostName,
            _options.MaxConcurrentJobs,
            _options.Queues,
            _registry.Capabilities,
            new Dictionary<string, string>(_options.Labels, StringComparer.Ordinal));

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var registration = await _runtimeClient.RegisterAsync(request, cancellationToken);
                Volatile.Write(ref _sessionEpoch, registration.SessionEpoch);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "KubeJob worker registration failed; retrying");
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
        }
    }

    private async Task ClaimLoopAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (Volatile.Read(ref _draining) != 0)
            {
                await Task.Delay(_options.EmptyPollDelay, stoppingToken);
                continue;
            }

            var availableSlots = _options.MaxConcurrentJobs - Volatile.Read(ref _reservedSlots);
            if (availableSlots <= 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), stoppingToken);
                continue;
            }

            try
            {
                var response = await _runtimeClient.ClaimAsync(
                    new ClaimJobsRequest(
                        _options.WorkerId,
                        _sessionId,
                        Volatile.Read(ref _sessionEpoch),
                        Math.Min(availableSlots, _options.ClaimBatchSize),
                        _options.Queues,
                        _registry.Capabilities),
                    stoppingToken);

                if (response.Jobs.Count == 0)
                {
                    await _claimTrigger.WaitAsync(
                        _options.EmptyPollDelay,
                        stoppingToken);
                    continue;
                }

                foreach (var job in response.Jobs)
                {
                    if (!_registry.TryGet(job.JobKey, out _))
                    {
                        await ReportAsync(
                            job,
                            JobAttemptOutcome.PermanentFailure,
                            "handler_not_registered",
                            $"Worker does not contain a handler for '{job.JobKey}'.",
                            stoppingToken);
                        continue;
                    }

                    var owned = new OwnedAttempt(job, _sessionId);
                    if (!_owned.TryAdd(job.AttemptId, owned))
                    {
                        owned.CancellationSource.Dispose();
                        continue;
                    }

                    Interlocked.Increment(ref _reservedSlots);
                    try
                    {
                        await _channel.Writer.WriteAsync(job, stoppingToken);
                    }
                    catch
                    {
                        ReleaseOwnedAttempt(job.AttemptId);

                        throw;
                    }
                }
            }
            catch (ChannelClosedException) when (Volatile.Read(ref _draining) != 0)
            {
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "KubeJob claim request failed");
                await Task.Delay(_options.EmptyPollDelay, stoppingToken);
            }
        }
    }

    private async Task ConsumeAsync(int consumerIndex, CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var job in _channel.Reader.ReadAllAsync(stoppingToken))
            {
                if (!_owned.TryGetValue(job.AttemptId, out var owned))
                {
                    continue;
                }

                var completionReported = false;
                try
                {
                    if (!_registry.TryGet(job.JobKey, out var handler))
                    {
                        completionReported = await ReportAsync(
                            job,
                            JobAttemptOutcome.PermanentFailure,
                            "handler_not_registered",
                            $"Worker does not contain a handler for '{job.JobKey}'.",
                            stoppingToken);
                        continue;
                    }

                    using var scope = _scopeFactory.CreateScope();
                    using var timeoutSource = new CancellationTokenSource(TimeSpan.FromSeconds(job.TimeoutSeconds));
                    using var executionSource = CancellationTokenSource.CreateLinkedTokenSource(
                        stoppingToken,
                        owned.CancellationSource.Token,
                        timeoutSource.Token);

                    var context = new JobExecutionContext
                    {
                        RunId = job.RunId,
                        AttemptId = job.AttemptId,
                        AttemptNumber = job.AttemptNumber,
                        StartedAt = DateTimeOffset.UtcNow,
                        Worker = new WorkerExecutionInfo(
                            _options.WorkerId,
                            _sessionId,
                            Volatile.Read(ref _sessionEpoch),
                            _hostName,
                            _options.BuildId)
                    };

                    _logger.LogInformation(
                        "Consumer {ConsumerIndex} executing job {RunId} attempt {AttemptNumber} ({JobKey})",
                        consumerIndex,
                        job.RunId,
                        job.AttemptNumber,
                        job.JobKey);

                    await handler.InvokeAsync(
                        scope.ServiceProvider,
                        job.PayloadJson,
                        context,
                        executionSource.Token);

                    completionReported = await ReportAsync(
                        job,
                        JobAttemptOutcome.Succeeded,
                        null,
                        null,
                        stoppingToken);
                }
                catch (OperationCanceledException) when (owned.CancellationSource.IsCancellationRequested)
                {
                    completionReported = await ReportAsync(
                        job,
                        JobAttemptOutcome.Canceled,
                        "canceled",
                        "Execution was canceled by the control plane, worker drain, or session fencing.",
                        stoppingToken);
                }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    completionReported = await ReportAsync(
                        job,
                        JobAttemptOutcome.TimedOut,
                        "timeout",
                        $"Execution exceeded its {job.TimeoutSeconds} second timeout.",
                        stoppingToken);
                }
                catch (System.Text.Json.JsonException ex)
                {
                    completionReported = await ReportAsync(
                        job,
                        JobAttemptOutcome.PermanentFailure,
                        "payload_invalid",
                        ex.Message,
                        stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "KubeJob attempt {AttemptId} failed", job.AttemptId);
                    completionReported = await ReportAsync(
                        job,
                        JobAttemptOutcome.RetryableFailure,
                        "handler_exception",
                        ex.ToString(),
                        stoppingToken);
                }
                finally
                {
                    owned.Completion.TrySetResult(completionReported);
                    ReleaseOwnedAttempt(job.AttemptId);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task RenewLoopAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.LeaseRenewalInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                var snapshot = _owned.Values.ToArray();
                if (snapshot.Length == 0)
                {
                    continue;
                }

                try
                {
                    var response = await _runtimeClient.RenewLeasesAsync(
                        new RenewLeasesRequest(
                            _options.WorkerId,
                            _sessionId,
                            Volatile.Read(ref _sessionEpoch),
                            snapshot.Select(x => new LeaseRenewal(
                                x.Job.AttemptId,
                                x.Job.LeaseToken)).ToArray()),
                        stoppingToken);

                    foreach (var renewal in response.Attempts)
                    {
                        if (_owned.TryGetValue(renewal.AttemptId, out var owned)
                            && (!renewal.Renewed || renewal.CancelRequested))
                        {
                            owned.CancellationSource.Cancel();
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "KubeJob lease renewal failed");
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task HeartbeatLoopAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.HeartbeatInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                bool accepted;
                try
                {
                    accepted = await SendHeartbeatAsync(
                        Volatile.Read(ref _draining) == 0
                            ? WorkerSessionState.Ready
                            : WorkerSessionState.Draining,
                        stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "KubeJob heartbeat failed");
                    continue;
                }

                if (!accepted)
                {
                    _logger.LogWarning(
                        "KubeJob worker session {WorkerId}/{SessionId}/{Epoch} was rejected; restarting session",
                        _options.WorkerId,
                        _sessionId,
                        Volatile.Read(ref _sessionEpoch));
                    FenceSession();
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async ValueTask<bool> SendHeartbeatAsync(
        WorkerSessionState state,
        CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _sessionEpoch) == 0)
        {
            return true;
        }

        return await _runtimeClient.HeartbeatAsync(
            new WorkerHeartbeatRequest(
                _options.WorkerId,
                _sessionId,
                Volatile.Read(ref _sessionEpoch),
                Math.Max(0, _options.MaxConcurrentJobs - Volatile.Read(ref _reservedSlots)),
                state),
            cancellationToken);
    }

    private async Task SendHeartbeatBestEffortAsync(
        WorkerSessionState state,
        CancellationToken cancellationToken)
    {
        try
        {
            await SendHeartbeatAsync(state, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "KubeJob heartbeat failed during shutdown");
        }
    }

    private void FenceSession()
    {
        Interlocked.Exchange(ref _draining, 1);
        _channel.Writer.TryComplete();
        CancelOwnedAttempts();
        Volatile.Read(ref _sessionLifetime)?.Cancel();
    }

    private void CancelOwnedAttempts()
    {
        foreach (var owned in _owned.Values)
        {
            TryCancelOwnedAttempt(owned);
        }
    }

    private void TryCancelOwnedAttempt(OwnedAttempt owned)
    {
        try
        {
            owned.CancellationSource.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Completion already won the race with cancellation cleanup.
        }
    }

    private void ReleaseOwnedAttempt(string attemptId)
    {
        if (!_owned.TryRemove(attemptId, out var removed))
        {
            return;
        }

        removed.CancellationSource.Dispose();
        if (string.Equals(removed.SessionId, _sessionId, StringComparison.Ordinal))
        {
            Interlocked.Decrement(ref _reservedSlots);
        }
    }

    private async Task CloseSessionBestEffortAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _sessionEpoch) == 0)
        {
            return;
        }

        try
        {
            await _runtimeClient.CloseAsync(
                new WorkerHeartbeatRequest(
                    _options.WorkerId,
                    _sessionId,
                    Volatile.Read(ref _sessionEpoch),
                    0,
                    WorkerSessionState.Closed),
                cancellationToken);
        }
        catch
        {
            // Lease expiry remains the correctness fallback.
        }
    }

    private async Task<bool> ReportAsync(
        ClaimedJob job,
        JobAttemptOutcome outcome,
        string? failureCode,
        string? failureMessage,
        CancellationToken cancellationToken)
    {
        var request = new CompleteAttemptRequest(
            _options.WorkerId,
            _sessionId,
            Volatile.Read(ref _sessionEpoch),
            job.RunId,
            job.AttemptId,
            job.AttemptNumber,
            job.LeaseToken,
            outcome,
            Truncate(failureCode, 200),
            Truncate(failureMessage, _options.MaximumFailureMessageLength));

        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                var result = await _runtimeClient.CompleteAsync(request, cancellationToken);
                if (result.Accepted || !string.IsNullOrWhiteSpace(result.RejectionReason))
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to report completion for attempt {AttemptId} (try {Try})",
                    job.AttemptId,
                    attempt);
            }

            try
            {
                await Task.Delay(GetJitteredBackoff(attempt), cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return false;
            }
        }

        _logger.LogError(
            "Unable to report completion for attempt {AttemptId}; waiting for lease reconciliation",
            job.AttemptId);
        return false;
    }

    private static TimeSpan GetJitteredBackoff(int attempt)
    {
        // ±20% jitter spreads retry waves so multiple workers reporting the
        // same failure don't synchronize their next attempt.
        var seconds = Math.Max(1, attempt);
        var jitter = 1.0 + ((Random.Shared.NextDouble() * 0.4) - 0.2);
        return TimeSpan.FromSeconds(seconds * jitter);
    }

    private static string? Truncate(string? value, int maximumLength)
    {
        if (string.IsNullOrEmpty(value) || value.Length <= maximumLength)
        {
            return value;
        }

        return value[..(maximumLength - TruncatedSuffix.Length)] + TruncatedSuffix;
    }

    private sealed class OwnedAttempt
    {
        public OwnedAttempt(ClaimedJob job, string sessionId)
        {
            Job = job;
            SessionId = sessionId;
        }

        public ClaimedJob Job { get; }

        public string SessionId { get; }

        public CancellationTokenSource CancellationSource { get; } = new();

        public TaskCompletionSource<bool> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
