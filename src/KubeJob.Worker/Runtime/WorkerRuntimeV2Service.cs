using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Threading.Channels;
using KubeJob.Core.Execution;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Runtime;

public sealed class WorkerRuntimeV2Service : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly JobHandlerRegistry _registry;
    private readonly KubeJobWorkerOptions _options;
    private readonly ILogger<WorkerRuntimeV2Service> _logger;
    private readonly HttpClient _httpClient;
    private readonly Channel<ClaimedJob> _channel;
    private readonly ConcurrentDictionary<string, OwnedAttempt> _owned = new(StringComparer.Ordinal);
    private readonly string _sessionId = Guid.NewGuid().ToString("N");

    private long _sessionEpoch;
    private int _reservedSlots;
    private int _draining;

    public WorkerRuntimeV2Service(
        IServiceScopeFactory scopeFactory,
        JobHandlerRegistry registry,
        IOptions<KubeJobWorkerOptions> options,
        ILogger<WorkerRuntimeV2Service> logger)
    {
        _scopeFactory = scopeFactory;
        _registry = registry;
        _options = options.Value;
        _logger = logger;
        _options.ValidateV2();

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(_options.ServerEndpoint, UriKind.Absolute),
            Timeout = TimeSpan.FromSeconds(30)
        };

        _channel = Channel.CreateBounded<ClaimedJob>(new BoundedChannelOptions(_options.MaxConcurrentJobs)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false,
            AllowSynchronousContinuations = false
        });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_registry.Capabilities.Count == 0)
        {
            throw new InvalidOperationException(
                "The V2 worker has no typed handlers. Register at least one AddKubeJobHandler<TJob, TPayload>.");
        }

        await RegisterSessionUntilAcceptedAsync(stoppingToken);
        _logger.LogInformation(
            "KubeJob V2 worker {WorkerId} session {SessionId}/{Epoch} started with capacity {Capacity}",
            _options.WorkerId,
            _sessionId,
            _sessionEpoch,
            _options.MaxConcurrentJobs);

        var consumers = Enumerable.Range(0, _options.MaxConcurrentJobs)
            .Select(index => ConsumeAsync(index, stoppingToken))
            .ToArray();

        var loops = new Task[]
        {
            ClaimLoopAsync(stoppingToken),
            RenewLoopAsync(stoppingToken),
            HeartbeatLoopAsync(stoppingToken)
        };

        await Task.WhenAll(consumers.Concat(loops));
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        Interlocked.Exchange(ref _draining, 1);
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
            foreach (var owned in _owned.Values)
            {
                owned.CancellationSource.Cancel();
            }
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
            Environment.MachineName,
            _options.MaxConcurrentJobs,
            _options.Queues.Distinct(StringComparer.Ordinal).ToArray(),
            _registry.Capabilities,
            new Dictionary<string, string>(_options.Labels, StringComparer.Ordinal));

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                using var response = await _httpClient.PostAsJsonAsync(
                    "api/kubejob/runtime/workers/register",
                    request,
                    cancellationToken);
                response.EnsureSuccessStatusCode();

                var registration = await response.Content.ReadFromJsonAsync<RegisterWorkerSessionResponse>(
                    cancellationToken: cancellationToken);
                if (registration is null)
                {
                    throw new InvalidOperationException("Worker registration returned an empty response.");
                }

                Volatile.Write(ref _sessionEpoch, registration.SessionEpoch);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "KubeJob V2 worker registration failed; retrying");
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
                var request = new ClaimJobsRequest(
                    _options.WorkerId,
                    _sessionId,
                    Volatile.Read(ref _sessionEpoch),
                    Math.Min(availableSlots, _options.ClaimBatchSize),
                    _options.Queues,
                    _registry.Capabilities);

                using var response = await _httpClient.PostAsJsonAsync(
                    "api/kubejob/runtime/claims",
                    request,
                    stoppingToken);
                response.EnsureSuccessStatusCode();

                var result = await response.Content.ReadFromJsonAsync<ClaimJobsResponse>(
                    cancellationToken: stoppingToken);
                if (result is null || result.Jobs.Count == 0)
                {
                    await Task.Delay(_options.EmptyPollDelay, stoppingToken);
                    continue;
                }

                foreach (var job in result.Jobs)
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

                    var owned = new OwnedAttempt(job);
                    if (!_owned.TryAdd(job.AttemptId, owned))
                    {
                        continue;
                    }

                    Interlocked.Increment(ref _reservedSlots);
                    await _channel.Writer.WriteAsync(job, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "KubeJob V2 claim request failed");
                await Task.Delay(_options.EmptyPollDelay, stoppingToken);
            }
        }
    }

    private async Task ConsumeAsync(int consumerIndex, CancellationToken stoppingToken)
    {
        await foreach (var job in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            if (!_owned.TryGetValue(job.AttemptId, out var owned))
            {
                continue;
            }

            try
            {
                if (!_registry.TryGet(job.JobKey, out var handler))
                {
                    await ReportAsync(
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
                        Environment.MachineName,
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

                await ReportAsync(job, JobAttemptOutcome.Succeeded, null, null, stoppingToken);
            }
            catch (OperationCanceledException) when (owned.CancellationSource.IsCancellationRequested)
            {
                await ReportAsync(
                    job,
                    JobAttemptOutcome.Canceled,
                    "canceled",
                    "Execution was canceled by the control plane or worker drain.",
                    stoppingToken);
            }
            catch (OperationCanceledException)
            {
                await ReportAsync(
                    job,
                    JobAttemptOutcome.TimedOut,
                    "timeout",
                    $"Execution exceeded its {job.TimeoutSeconds} second timeout.",
                    stoppingToken);
            }
            catch (System.Text.Json.JsonException ex)
            {
                await ReportAsync(
                    job,
                    JobAttemptOutcome.PermanentFailure,
                    "payload_invalid",
                    ex.Message,
                    stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KubeJob attempt {AttemptId} failed", job.AttemptId);
                await ReportAsync(
                    job,
                    JobAttemptOutcome.RetryableFailure,
                    "handler_exception",
                    ex.ToString(),
                    stoppingToken);
            }
            finally
            {
                if (_owned.TryRemove(job.AttemptId, out var removed))
                {
                    removed.CancellationSource.Dispose();
                    Interlocked.Decrement(ref _reservedSlots);
                }
            }
        }
    }

    private async Task RenewLoopAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.LeaseRenewalInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var snapshot = _owned.Values.ToArray();
            if (snapshot.Length == 0)
            {
                continue;
            }

            try
            {
                var request = new RenewLeasesRequest(
                    _options.WorkerId,
                    _sessionId,
                    Volatile.Read(ref _sessionEpoch),
                    snapshot.Select(x => new LeaseRenewal(x.Job.AttemptId, x.Job.LeaseToken)).ToArray());

                using var response = await _httpClient.PostAsJsonAsync(
                    "api/kubejob/runtime/leases/renew",
                    request,
                    stoppingToken);
                response.EnsureSuccessStatusCode();

                var renewals = await response.Content.ReadFromJsonAsync<RenewLeasesResponse>(
                    cancellationToken: stoppingToken);
                if (renewals is null)
                {
                    continue;
                }

                foreach (var renewal in renewals.Attempts)
                {
                    if (!_owned.TryGetValue(renewal.AttemptId, out var owned))
                    {
                        continue;
                    }

                    if (!renewal.Renewed || renewal.CancelRequested)
                    {
                        owned.CancellationSource.Cancel();
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "KubeJob lease renewal failed; attempts remain fenced by their lease tokens");
            }
        }
    }

    private async Task HeartbeatLoopAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.HeartbeatInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await SendHeartbeatBestEffortAsync(
                Volatile.Read(ref _draining) == 0
                    ? WorkerSessionState.Ready
                    : WorkerSessionState.Draining,
                stoppingToken);
        }
    }

    private async Task SendHeartbeatBestEffortAsync(
        WorkerSessionState state,
        CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _sessionEpoch) == 0)
        {
            return;
        }

        try
        {
            var request = new WorkerHeartbeatRequest(
                _options.WorkerId,
                _sessionId,
                Volatile.Read(ref _sessionEpoch),
                Math.Max(0, _options.MaxConcurrentJobs - Volatile.Read(ref _reservedSlots)),
                state);
            using var response = await _httpClient.PostAsJsonAsync(
                "api/kubejob/runtime/workers/heartbeat",
                request,
                cancellationToken);

            if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
            {
                foreach (var owned in _owned.Values)
                {
                    owned.CancellationSource.Cancel();
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "KubeJob heartbeat failed");
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
            var request = new WorkerHeartbeatRequest(
                _options.WorkerId,
                _sessionId,
                Volatile.Read(ref _sessionEpoch),
                0,
                WorkerSessionState.Closed);
            using var _ = await _httpClient.PostAsJsonAsync(
                "api/kubejob/runtime/workers/close",
                request,
                cancellationToken);
        }
        catch
        {
            // Lease expiry remains the correctness fallback.
        }
    }

    private async Task ReportAsync(
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
            failureCode,
            failureMessage);

        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                using var response = await _httpClient.PostAsJsonAsync(
                    "api/kubejob/runtime/attempts/complete",
                    request,
                    cancellationToken);

                if (response.IsSuccessStatusCode
                    || response.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    return;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to report completion for attempt {AttemptId} (try {Try})",
                    job.AttemptId,
                    attempt);
            }

            await Task.Delay(TimeSpan.FromSeconds(attempt), cancellationToken);
        }

        // No acknowledgement was obtained. The lease reaper will safely requeue the run.
        _logger.LogError(
            "Unable to report completion for attempt {AttemptId}; waiting for lease reconciliation",
            job.AttemptId);
    }

    private sealed class OwnedAttempt
    {
        public OwnedAttempt(ClaimedJob job)
        {
            Job = job;
        }

        public ClaimedJob Job { get; }

        public CancellationTokenSource CancellationSource { get; } = new();
    }
}
