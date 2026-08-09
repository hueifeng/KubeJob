using System.Collections.Concurrent;
using System.Threading.Channels;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Runtime;

/// <summary>
/// PostgresManaged worker coordinator. PostgreSQL owns Run/Attempt/Lease state;
/// this service only coordinates managed sessions, claims, lease renewal,
/// execution and durable completion. BrokerNative uses a transport adapter plus
/// the shared <see cref="IWorkerExecutionEngine"/> and never enters this loop.
/// </summary>
public sealed class WorkerRuntimeService : BackgroundService
{
    private const string TruncatedSuffix = "\n...[truncated]";

    private readonly JobHandlerRegistry _registry;
    private readonly IWorkerRuntimeClient _runtimeClient;
    private readonly IWorkerClaimTrigger _claimTrigger;
    private readonly KubeJobWorkerOptions _options;
    private readonly KubeJobWorkerMetrics? _metrics;
    private readonly ILogger<WorkerRuntimeService> _logger;
    private readonly IWorkerExecutionEngine _executionEngine;
    private readonly ConcurrentDictionary<string, OwnedAttempt> _owned = new(StringComparer.Ordinal);
    private readonly string _hostName = Environment.MachineName;

    private Channel<ClaimedJob> _channel;
    private CancellationTokenSource? _sessionLifetime;
    private string _sessionId = Guid.NewGuid().ToString("N");
    private long _sessionEpoch;
    private int _reservedSlots;
    private int _draining;
    private int _fenced;
    private int _stopRequested;
    private TaskCompletionSource _fenceDeadline = CreateFenceDeadlineSource();

    public WorkerRuntimeService(
        IServiceScopeFactory scopeFactory,
        JobHandlerRegistry registry,
        IWorkerRuntimeClient runtimeClient,
        IWorkerClaimTrigger claimTrigger,
        IOptions<KubeJobWorkerOptions> options,
        ILogger<WorkerRuntimeService> logger,
        KubeJobWorkerMetrics? metrics = null,
        JobExecutionPipelineBuilder? pipelineBuilder = null)
    {
        _registry = registry;
        _runtimeClient = runtimeClient;
        _claimTrigger = claimTrigger;
        _options = options.Value;
        _metrics = metrics;
        _logger = logger;
        _options.Validate();
        _executionEngine = new WorkerExecutionEngine(
            scopeFactory,
            registry,
            logger,
            metrics,
            pipelineBuilder);
        _channel = CreateExecutionChannel();
    }

    public string SessionId => _sessionId;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_registry.Capabilities.Count == 0)
        {
            throw new InvalidOperationException(
                "The worker has no typed handlers. Register at least one AddKubeJobHandler<TJob, TPayload>.");
        }

        while (!stoppingToken.IsCancellationRequested && Volatile.Read(ref _stopRequested) == 0)
        {
            PrepareNextSession();
            try
            {
                var runTask = RunSessionAsync(stoppingToken);
                var finished = await Task.WhenAny(runTask, _fenceDeadline.Task);
                if (finished == runTask)
                {
                    await runTask;
                }
                else
                {
                    TearDownSession();
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KubeJob managed worker session loop failed");
            }

            if (Volatile.Read(ref _fenced) != 0)
            {
                throw new InvalidOperationException(
                    "KubeJob managed worker session was fenced by the control plane; " +
                    "failing the hosted service so the process supervisor can restart it.");
            }

            if (!stoppingToken.IsCancellationRequested && Volatile.Read(ref _stopRequested) == 0)
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
            _logger.LogInformation(
                "KubeJob managed worker {WorkerId} session {SessionId}/{Epoch} started with capacity {Capacity}",
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
            TearDownSession();
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
        _fenceDeadline = CreateFenceDeadlineSource();
        _channel = CreateExecutionChannel();
    }

    private void TearDownSession()
    {
        var sessionLifetime = Volatile.Read(ref _sessionLifetime);
        if (sessionLifetime is not null)
        {
            sessionLifetime.Cancel();
            Interlocked.CompareExchange(ref _sessionLifetime, null, sessionLifetime);
        }

        _channel.Writer.TryComplete();
        foreach (var owned in _owned.Values
                     .Where(owned => string.Equals(owned.SessionId, _sessionId, StringComparison.Ordinal))
                     .ToArray())
        {
            TryCancelOwnedAttempt(owned);
            ReleaseOwnedAttempt(owned.Job.AttemptId);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        Interlocked.Exchange(ref _stopRequested, 1);
        Interlocked.Exchange(ref _draining, 1);
        _channel.Writer.TryComplete();
        await SendHeartbeatBestEffortAsync(WorkerSessionState.Draining, cancellationToken);

        var deadline = DateTimeOffset.UtcNow.Add(_options.DrainTimeout);
        while (!_owned.IsEmpty && DateTimeOffset.UtcNow < deadline)
        {
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }

        if (!_owned.IsEmpty)
        {
            CancelOwnedAttempts();
        }

        Volatile.Read(ref _sessionLifetime)?.Cancel();
        await CloseSessionBestEffortAsync(CancellationToken.None);
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
            new Dictionary<string, string>(_options.Labels, StringComparer.Ordinal),
            _options.ConsumerGroup,
            _options.ExecutionLane);

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
                _logger.LogWarning(ex, "KubeJob managed worker registration failed; retrying");
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
                        _registry.Capabilities,
                        ConsumerGroup: _options.ConsumerGroup,
                        ExecutionLane: _options.ExecutionLane),
                    stoppingToken);

                if (response.Jobs.Count == 0)
                {
                    await _claimTrigger.WaitAsync(_options.EmptyPollDelay, stoppingToken);
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
                    _metrics?.AttemptStarted(WorkerExecutionKind.Pull);
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
                _logger.LogWarning(ex, "KubeJob managed claim request failed");
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

                try
                {
                    WorkerExecutionResult execution;
                    try
                    {
                        execution = await _executionEngine.ExecuteAsync(
                            new WorkerExecutionRequest(
                                job.RunId,
                                job.AttemptId,
                                job.AttemptNumber,
                                job.JobKey,
                                job.PayloadJson,
                                job.TimeoutSeconds,
                                new WorkerExecutionInfo(
                                    _options.WorkerId,
                                    _sessionId,
                                    Volatile.Read(ref _sessionEpoch),
                                    _hostName,
                                    _options.BuildId),
                                owned.CancellationSource.Token,
                                stoppingToken,
                                consumerIndex,
                                job.LeaseToken,
                                job.FenceVersion));
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "KubeJob execution engine failed for attempt {AttemptId}",
                            job.AttemptId);
                        execution = new WorkerExecutionResult(
                            JobAttemptOutcome.RetryableFailure,
                            "execution_engine_exception",
                            ex.ToString());
                    }

                    await ReportAsync(
                        job,
                        execution.Outcome,
                        execution.FailureCode,
                        execution.FailureMessage,
                        stoppingToken);
                }
                finally
                {
                    ReleaseOwnedAttempt(job.AttemptId);
                }

                if (stoppingToken.IsCancellationRequested)
                {
                    break;
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
                                x.Job.LeaseToken,
                                x.Job.FenceVersion)).ToArray()),
                        stoppingToken);

                    foreach (var renewal in response.Attempts)
                    {
                        if (_owned.TryGetValue(renewal.AttemptId, out var owned)
                            && (!renewal.Renewed || renewal.CancelRequested))
                        {
                            TryCancelOwnedAttempt(owned);
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "KubeJob managed lease renewal failed");
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
                    _logger.LogDebug(ex, "KubeJob managed heartbeat failed");
                    continue;
                }

                if (!accepted)
                {
                    _logger.LogWarning(
                        "KubeJob managed worker session {WorkerId}/{SessionId}/{Epoch} was rejected",
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
        Interlocked.Exchange(ref _fenced, 1);
        Interlocked.Exchange(ref _draining, 1);
        _channel.Writer.TryComplete();
        CancelOwnedAttempts();
        Volatile.Read(ref _sessionLifetime)?.Cancel();
        _ = ForceFenceDeadlineAsync();
    }

    private async Task ForceFenceDeadlineAsync()
    {
        await Task.Delay(_options.DrainTimeout);
        _fenceDeadline.TrySetResult();
    }

    private void CancelOwnedAttempts()
    {
        foreach (var owned in _owned.Values)
        {
            TryCancelOwnedAttempt(owned);
        }
    }

    private static void TryCancelOwnedAttempt(OwnedAttempt owned)
    {
        try
        {
            owned.CancellationSource.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private void ReleaseOwnedAttempt(string attemptId)
    {
        if (!_owned.TryRemove(attemptId, out var removed))
        {
            return;
        }

        removed.CancellationSource.Dispose();
        _metrics?.AttemptFinished(WorkerExecutionKind.Pull);
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
            Truncate(failureMessage, _options.MaximumFailureMessageLength),
            job.FenceVersion);

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

    private Channel<ClaimedJob> CreateExecutionChannel() =>
        Channel.CreateBounded<ClaimedJob>(new BoundedChannelOptions(_options.MaxConcurrentJobs)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false,
            AllowSynchronousContinuations = false
        });

    private static TaskCompletionSource CreateFenceDeadlineSource() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static TimeSpan GetJitteredBackoff(int attempt)
    {
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
    }
}
