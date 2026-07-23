using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using KubeJob.Core.Context;
using KubeJob.Core.Domain;
using KubeJob.Core.Dtos;
using KubeJob.Core.Enums;
using KubeJob.Worker.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Runtime;

/// <summary>
/// Fixed-slot worker: startup-only reflection, bounded local queue, reusable heartbeat DTOs and
/// fenced lease ownership. HTTP/JSON/DI allocations remain bounded and proportional to real work.
/// </summary>
public sealed partial class WorkerRuntimeV2Service : BackgroundService
{
    private static readonly TimeSpan ErrorBackoff = TimeSpan.FromSeconds(2);
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IKubeJobRuntimeClient _client;
    private readonly JobRegistry _registry;
    private readonly KubeJobRuntimeV2WorkerOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<WorkerRuntimeV2Service> _logger;
    private readonly Channel<OwnedLease> _queue;
    private readonly WorkerSlot[] _slots;
    private readonly List<LeaseRenewalDto> _renewalItems;
    private readonly ConcurrentDictionary<string, OwnedLease> _owned;
    private readonly ClaimRunsRequest _claimRequest;
    private readonly RenewLeasesRequest _renewRequest;
    private readonly string _workerId;
    private readonly string _sessionId = Guid.NewGuid().ToString("N");

    private long _sessionEpoch;
    private TimeSpan _heartbeatInterval = TimeSpan.FromSeconds(5);
    private int _queued;
    private int _running;
    private int _draining;

    public WorkerRuntimeV2Service(
        IServiceScopeFactory scopeFactory,
        IKubeJobRuntimeClient client,
        JobRegistry registry,
        IOptions<KubeJobRuntimeV2WorkerOptions> options,
        TimeProvider timeProvider,
        ILogger<WorkerRuntimeV2Service> logger)
    {
        _scopeFactory = scopeFactory;
        _client = client;
        _registry = registry;
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
        ValidateOptions(_options);

        _workerId = string.IsNullOrWhiteSpace(_options.WorkerId) ? Environment.MachineName : _options.WorkerId;
        _queue = Channel.CreateBounded<OwnedLease>(new BoundedChannelOptions(_options.MaxConcurrentJobs)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = _options.MaxConcurrentJobs == 1,
            AllowSynchronousContinuations = false
        });

        _slots = new WorkerSlot[_options.MaxConcurrentJobs];
        for (var index = 0; index < _slots.Length; index++)
            _slots[index] = new WorkerSlot(_workerId, _sessionId);

        _claimRequest = new ClaimRunsRequest
        {
            WorkerId = _workerId,
            SessionId = _sessionId,
            QueueNames = _options.QueueNames.Where(static x => !string.IsNullOrWhiteSpace(x))
                .Distinct(StringComparer.Ordinal).ToArray(),
            WaitMilliseconds = (int)Math.Round(_options.LongPollTimeout.TotalMilliseconds)
        };
        _owned = new ConcurrentDictionary<string, OwnedLease>(
            concurrencyLevel: Math.Min(Environment.ProcessorCount, _options.MaxConcurrentJobs),
            capacity: _options.MaxConcurrentJobs,
            comparer: StringComparer.Ordinal);
        _renewalItems = new List<LeaseRenewalDto>(_slots.Length);
        _renewRequest = new RenewLeasesRequest
        {
            WorkerId = _workerId,
            SessionId = _sessionId,
            Leases = _renewalItems
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await RegisterWithRetryAsync(stoppingToken);
        var loops = new Task[_slots.Length + 2];
        loops[0] = ClaimLoopAsync(stoppingToken);
        loops[1] = RenewLoopAsync(stoppingToken);
        for (var index = 0; index < _slots.Length; index++)
            loops[index + 2] = ExecuteSlotLoopAsync(index, stoppingToken);
        await Task.WhenAll(loops);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        Interlocked.Exchange(ref _draining, 1);
        _queue.Writer.TryComplete();
        try { await SendDrainSignalAsync(cancellationToken); }
        catch (Exception ex) when (ex is not OperationCanceledException) { DrainSignalFailed(_logger, ex); }

        using var grace = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        grace.CancelAfter(_options.ShutdownGracePeriod);
        try
        {
            while (Volatile.Read(ref _queued) != 0 || Volatile.Read(ref _running) != 0)
                await Task.Delay(50, grace.Token);
        }
        catch (OperationCanceledException)
        {
            CancelAllSlots(CancellationReason.WorkerShutdown);
        }
        await base.StopAsync(cancellationToken);
    }

    private async Task RegisterWithRetryAsync(CancellationToken cancellationToken)
    {
        var jobs = _registry.Jobs.ToArray();
        var capabilities = new WorkerCapabilityDto[jobs.Length];
        var definitions = new JobDefinitionDto[jobs.Length];
        for (var i = 0; i < jobs.Length; i++)
        {
            var job = jobs[i];
            capabilities[i] = new WorkerCapabilityDto
            {
                JobType = job.Name,
                HandlerVersion = job.HandlerVersion,
                PayloadSchemaVersion = job.PayloadSchemaVersion
            };
            definitions[i] = new JobDefinitionDto
            {
                Name = job.Name,
                Cron = job.Cron,
                ExecuteModel = job.ExecuteModel,
                TotalShards = job.TotalShards,
                TimeoutSeconds = job.TimeoutSeconds,
                MaxRetries = job.MaxRetries,
                NodeSelectors = job.NodeSelectors
            };
        }

        var request = new RegisterWorkerSessionRequest
        {
            WorkerId = _workerId,
            SessionId = _sessionId,
            RuntimeVersion = typeof(WorkerRuntimeV2Service).Assembly.GetName().Version?.ToString() ?? string.Empty,
            Labels = _options.Labels,
            Capabilities = capabilities,
            Definitions = definitions,
            MaxCapacity = _options.MaxConcurrentJobs
        };

        var delay = TimeSpan.FromMilliseconds(250);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var response = await _client.RegisterAsync(request, cancellationToken);
                _sessionEpoch = response.SessionEpoch;
                _heartbeatInterval = response.HeartbeatInterval > TimeSpan.Zero
                    ? response.HeartbeatInterval : TimeSpan.FromSeconds(5);
                _claimRequest.SessionEpoch = _sessionEpoch;
                _renewRequest.SessionEpoch = _sessionEpoch;
                foreach (var slot in _slots)
                    slot.Complete.SessionEpoch = _sessionEpoch;
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception ex)
            {
                RegistrationFailed(_logger, delay.TotalMilliseconds, ex);
                await Task.Delay(delay, cancellationToken);
                delay = TimeSpan.FromMilliseconds(Math.Min(10_000, delay.TotalMilliseconds * 2));
            }
        }
    }

    private async Task ClaimLoopAsync(CancellationToken cancellationToken)
    {
        var emptyDelay = _options.MinEmptyClaimDelay;
        while (!cancellationToken.IsCancellationRequested && Volatile.Read(ref _draining) == 0)
        {
            var available = _options.MaxConcurrentJobs - Volatile.Read(ref _running) - Volatile.Read(ref _queued);
            if (available <= 0)
            {
                await Task.Delay(_options.MinEmptyClaimDelay, cancellationToken);
                continue;
            }

            try
            {
                _claimRequest.AvailableSlots = Math.Min(available, _options.MaxServerClaimBatch);
                var response = await _client.ClaimAsync(_claimRequest, cancellationToken);
                if (response.Leases.Count == 0)
                {
                    await Task.Delay(AddJitter(emptyDelay), cancellationToken);
                    emptyDelay = NextEmptyDelay(emptyDelay);
                    continue;
                }

                emptyDelay = _options.MinEmptyClaimDelay;
                foreach (var lease in response.Leases)
                {
                    var owned = new OwnedLease(lease);
                    if (!_owned.TryAdd(lease.RunId, owned)) continue;
                    Interlocked.Increment(ref _queued);
                    try { await _queue.Writer.WriteAsync(owned, cancellationToken); }
                    catch
                    {
                        _owned.TryRemove(lease.RunId, out _);
                        Interlocked.Decrement(ref _queued);
                        throw;
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { break; }
            catch (ChannelClosedException) { break; }
            catch (Exception ex)
            {
                ClaimFailed(_logger, ex);
                await Task.Delay(ErrorBackoff, cancellationToken);
            }
        }
    }

    private async Task ExecuteSlotLoopAsync(int index, CancellationToken stoppingToken)
    {
        var slot = _slots[index];
        await foreach (var owned in _queue.Reader.ReadAllAsync(stoppingToken))
        {
            var lease = owned.Lease;
            Interlocked.Decrement(ref _queued);
            var queuedReason = (CancellationReason)Volatile.Read(ref owned.CancellationReasonValue);
            if (queuedReason != CancellationReason.None)
            {
                if (queuedReason == CancellationReason.ControlPlane)
                    await CompleteWithRetryAsync(lease, slot, JobStatus.Canceled,
                        "Canceled by control plane before handler start.", CancellationToken.None);
                _owned.TryRemove(lease.RunId, out _);
                continue;
            }

            Interlocked.Increment(ref _running);
            using var executionCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            Volatile.Write(ref slot.CancellationReasonValue, (int)CancellationReason.None);
            Volatile.Write(ref slot.Lease, lease);
            Volatile.Write(ref slot.Cancellation, executionCts);
            queuedReason = (CancellationReason)Volatile.Read(ref owned.CancellationReasonValue);
            if (queuedReason != CancellationReason.None)
            {
                Volatile.Write(ref slot.CancellationReasonValue, (int)queuedReason);
                executionCts.Cancel();
            }
            try { await ExecuteLeaseAsync(lease, slot, executionCts, stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception ex) { ExecutionLoopFailed(_logger, lease.RunId, ex); }
            finally
            {
                Volatile.Write(ref slot.Cancellation, null);
                Volatile.Write(ref slot.Lease, null);
                Volatile.Write(ref slot.CancellationReasonValue, (int)CancellationReason.None);
                _owned.TryRemove(lease.RunId, out _);
                Interlocked.Decrement(ref _running);
            }
        }
    }

    private async Task ExecuteLeaseAsync(JobLease lease, WorkerSlot slot,
        CancellationTokenSource executionCts, CancellationToken stoppingToken)
    {
        var reasonBeforeStart = (CancellationReason)Volatile.Read(ref slot.CancellationReasonValue);
        if (reasonBeforeStart == CancellationReason.ControlPlane)
        {
            await CompleteWithRetryAsync(lease, slot, JobStatus.Canceled,
                "Canceled by control plane before handler start.", CancellationToken.None);
            return;
        }
        if (reasonBeforeStart is CancellationReason.LeaseRejected or CancellationReason.WorkerShutdown)
            return;

        if (!_registry.TryGet(lease.JobType, out var descriptor))
        {
            await CompleteWithRetryAsync(lease, slot, JobStatus.Failed,
                $"Handler '{lease.JobType}' is not registered.", CancellationToken.None);
            return;
        }

        var timeout = TimeSpan.FromSeconds(Math.Max(1, lease.TimeoutSeconds));
        executionCts.CancelAfter(timeout);
        var byteCount = Encoding.UTF8.GetByteCount(lease.PayloadJson);
        var payload = ArrayPool<byte>.Shared.Rent(Math.Max(1, byteCount));
        var length = Encoding.UTF8.GetBytes(lease.PayloadJson.AsSpan(), payload.AsSpan());
        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();
            var handler = scope.ServiceProvider.GetRequiredService(descriptor.HandlerType);
            var logger = scope.ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger(descriptor.HandlerType);
            var context = new KubeJobContextV2
            {
                RunId = lease.RunId,
                SpecId = lease.SpecId,
                BatchId = lease.BatchId,
                WorkerId = _workerId,
                WorkerSessionEpoch = _sessionEpoch,
                LeaseToken = lease.LeaseToken,
                Attempt = lease.Attempt,
                ShardIndex = lease.ShardIndex,
                TotalShards = Math.Max(1, lease.TotalShards),
                ScheduledAt = lease.ScheduledAt,
                Deadline = _timeProvider.GetUtcNow() + timeout,
                PayloadUtf8 = payload.AsMemory(0, length),
                Services = scope.ServiceProvider,
                Logger = logger
            };
            await descriptor.InvokeAsync(handler, context, executionCts.Token);
            await CompleteWithRetryAsync(lease, slot, JobStatus.Succeeded, string.Empty, CancellationToken.None);
        }
        catch (OperationCanceledException) when (executionCts.IsCancellationRequested)
        {
            var reason = (CancellationReason)Volatile.Read(ref slot.CancellationReasonValue);
            if (reason == CancellationReason.ControlPlane)
                await CompleteWithRetryAsync(lease, slot, JobStatus.Canceled,
                    "Canceled by control plane.", CancellationToken.None);
            else if (reason == CancellationReason.None && !stoppingToken.IsCancellationRequested)
                await CompleteWithRetryAsync(lease, slot, JobStatus.Failed,
                    "Job execution timed out.", CancellationToken.None);
            // Never write terminal state after lease rejection or forced process shutdown.
        }
        catch (Exception ex)
        {
            await CompleteWithRetryAsync(lease, slot, JobStatus.Failed, BuildExceptionSummary(ex), CancellationToken.None);
            ExecutionFailed(_logger, lease.RunId, ex);
        }
        finally { ArrayPool<byte>.Shared.Return(payload, clearArray: false); }
    }

    private async Task CompleteWithRetryAsync(JobLease lease, WorkerSlot slot, JobStatus status,
        string summary, CancellationToken cancellationToken)
    {
        slot.Complete.RunId = lease.RunId;
        slot.Complete.LeaseToken = lease.LeaseToken;
        slot.Complete.Status = status;
        slot.Complete.ResultSummary = summary.Length <= 2000 ? summary : summary[..2000];
        for (var attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                if (await _client.CompleteAsync(slot.Complete, cancellationToken)) return;
                CompletionRejected(_logger, lease.RunId, lease.LeaseToken);
                return;
            }
            catch (Exception ex)
            {
                if (attempt == 2)
                {
                    CompletionAbandoned(_logger, lease.RunId, lease.LeaseToken, ex);
                    return;
                }

                CompletionRetry(_logger, lease.RunId, attempt + 1, ex);
                await Task.Delay(200 * (attempt + 1), cancellationToken);
            }
        }
    }

    private async Task RenewLoopAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_heartbeatInterval, _timeProvider);
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            try
            {
                var response = await RenewOnceAsync(cancellationToken);
                CancelMatchingSlots(response.RejectedRunIds, CancellationReason.LeaseRejected);
                CancelMatchingSlots(response.CancelRunIds, CancellationReason.ControlPlane);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { break; }
            catch (Exception ex) { RenewFailed(_logger, ex); }
        }
    }

    private Task<RenewLeasesResponse> RenewOnceAsync(CancellationToken cancellationToken)
    {
        _renewalItems.Clear();
        foreach (var pair in _owned)
            _renewalItems.Add(pair.Value.Renewal);
        _renewRequest.CurrentLoad = _owned.Count;
        _renewRequest.Draining = Volatile.Read(ref _draining) != 0;
        return _client.RenewAsync(_renewRequest, cancellationToken);
    }

    private Task<RenewLeasesResponse> SendDrainSignalAsync(CancellationToken cancellationToken)
    {
        var leases = new List<LeaseRenewalDto>(_owned.Count);
        foreach (var pair in _owned)
            leases.Add(pair.Value.Renewal);
        return _client.RenewAsync(new RenewLeasesRequest
        {
            WorkerId = _workerId,
            SessionId = _sessionId,
            SessionEpoch = _sessionEpoch,
            Leases = leases,
            CurrentLoad = _owned.Count,
            Draining = true
        }, cancellationToken);
    }

    private void CancelMatchingSlots(IReadOnlyList<string> runIds, CancellationReason reason)
    {
        foreach (var runId in runIds)
        {
            if (_owned.TryGetValue(runId, out var owned))
                Volatile.Write(ref owned.CancellationReasonValue, (int)reason);
            foreach (var slot in _slots)
            {
                var lease = Volatile.Read(ref slot.Lease);
                if (lease is null || !string.Equals(lease.RunId, runId, StringComparison.Ordinal)) continue;
                Volatile.Write(ref slot.CancellationReasonValue, (int)reason);
                Volatile.Read(ref slot.Cancellation)?.Cancel();
                break;
            }
        }
    }

    private void CancelAllSlots(CancellationReason reason)
    {
        foreach (var slot in _slots)
        {
            Volatile.Write(ref slot.CancellationReasonValue, (int)reason);
            Volatile.Read(ref slot.Cancellation)?.Cancel();
        }
    }

    private TimeSpan NextEmptyDelay(TimeSpan current) => TimeSpan.FromMilliseconds(Math.Min(
        _options.MaxEmptyClaimDelay.TotalMilliseconds,
        Math.Max(_options.MinEmptyClaimDelay.TotalMilliseconds, current.TotalMilliseconds * 2)));
    private static TimeSpan AddJitter(TimeSpan delay) => delay + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 31));
    private static string BuildExceptionSummary(Exception ex)
    {
        var name = ex.GetType().FullName ?? ex.GetType().Name;
        var max = Math.Max(0, 1998 - name.Length);
        var message = ex.Message.Length <= max ? ex.Message : ex.Message[..max];
        return string.Concat(name, ": ", message);
    }

    private static void ValidateOptions(KubeJobRuntimeV2WorkerOptions options)
    {
        if (options.MaxConcurrentJobs is <= 0 or > 4096) throw new InvalidOperationException("MaxConcurrentJobs must be 1..4096.");
        if (!Uri.TryCreate(options.ServerEndpoint, UriKind.Absolute, out _)) throw new InvalidOperationException("ServerEndpoint must be absolute.");
        if (options.LongPollTimeout < TimeSpan.Zero || options.LongPollTimeout > TimeSpan.FromSeconds(25))
            throw new InvalidOperationException("LongPollTimeout must be 0..25 seconds.");
        if (options.RequestTimeout <= options.LongPollTimeout + TimeSpan.FromSeconds(2))
            throw new InvalidOperationException("RequestTimeout must exceed LongPollTimeout by at least two seconds.");
        if (options.MinEmptyClaimDelay <= TimeSpan.Zero || options.MaxEmptyClaimDelay < options.MinEmptyClaimDelay)
            throw new InvalidOperationException("Empty claim delays are invalid.");
        if (options.MaxServerClaimBatch is <= 0 or > 256) throw new InvalidOperationException("MaxServerClaimBatch must be 1..256.");
        if (options.QueueNames.Length > 64 || options.QueueNames.Any(static x => string.IsNullOrWhiteSpace(x) || x.Length > 100))
            throw new InvalidOperationException("QueueNames are invalid.");
    }

    private enum CancellationReason { None, ControlPlane, LeaseRejected, WorkerShutdown }

    private sealed class OwnedLease
    {
        public OwnedLease(JobLease lease)
        {
            Lease = lease;
            Renewal = new LeaseRenewalDto { RunId = lease.RunId, LeaseToken = lease.LeaseToken };
        }
        public JobLease Lease { get; }
        public LeaseRenewalDto Renewal { get; }
        public int CancellationReasonValue;
    }

    private sealed class WorkerSlot
    {
        public WorkerSlot(string workerId, string sessionId)
        {
            Complete.WorkerId = workerId; Complete.SessionId = sessionId;
        }
        public JobLease? Lease;
        public CancellationTokenSource? Cancellation;
        public int CancellationReasonValue;
        public CompleteRunRequest Complete { get; } = new();
    }

    [LoggerMessage(1001, LogLevel.Warning, "Claim request failed.")]
    private static partial void ClaimFailed(ILogger logger, Exception exception);
    [LoggerMessage(1002, LogLevel.Warning, "Lease {LeaseToken} for run {RunId} is stale.")]
    private static partial void StaleLease(ILogger logger, string runId, long leaseToken);
    [LoggerMessage(1003, LogLevel.Error, "Execution of run {RunId} failed.")]
    private static partial void ExecutionFailed(ILogger logger, string runId, Exception exception);
    [LoggerMessage(1004, LogLevel.Warning, "Completion for run {RunId}, lease {LeaseToken}, was rejected.")]
    private static partial void CompletionRejected(ILogger logger, string runId, long leaseToken);
    [LoggerMessage(1005, LogLevel.Warning, "Completion retry {Attempt} for run {RunId} failed.")]
    private static partial void CompletionRetry(ILogger logger, string runId, int attempt, Exception exception);
    [LoggerMessage(1010, LogLevel.Error, "Completion for run {RunId}, lease {LeaseToken}, could not be delivered; the lease will expire and be retried.")]
    private static partial void CompletionAbandoned(ILogger logger, string runId, long leaseToken, Exception exception);
    [LoggerMessage(1006, LogLevel.Warning, "Lease renewal failed.")]
    private static partial void RenewFailed(ILogger logger, Exception exception);
    [LoggerMessage(1007, LogLevel.Warning, "Registration failed; retrying in {DelayMs} ms.")]
    private static partial void RegistrationFailed(ILogger logger, double delayMs, Exception exception);
    [LoggerMessage(1008, LogLevel.Warning, "Execution loop for run {RunId} failed before a safe terminal report.")]
    private static partial void ExecutionLoopFailed(ILogger logger, string runId, Exception exception);
    [LoggerMessage(1009, LogLevel.Warning, "Failed to signal draining state.")]
    private static partial void DrainSignalFailed(ILogger logger, Exception exception);
}
