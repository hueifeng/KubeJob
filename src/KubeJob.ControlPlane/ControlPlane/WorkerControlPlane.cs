using System.Diagnostics;
using KubeJob.ControlPlane.Telemetry;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.ControlPlane;

/// <summary>
/// Owns worker-session, claim, lease, and completion orchestration. HTTP and
/// in-process worker transports invoke the same implementation.
/// </summary>
public sealed class WorkerControlPlane
{
    private readonly IWorkerSessionStore _sessions;
    private readonly IJobClaimStore _claims;
    private readonly IJobCompletionStore _completions;
    private readonly IJobQueryStore _queries;
    private readonly IJobSubmissionStore _submissions;
    private readonly CompletionBatcher? _completionBatcher;
    private readonly JobRuntimeOptions _options;
    private readonly QueueCatalog _queueCatalog;
    private readonly KubeJobControlPlaneMetrics? _metrics;

    public WorkerControlPlane(
        IWorkerSessionStore sessions,
        IJobClaimStore claims,
        IJobCompletionStore completions,
        IJobQueryStore queries,
        IJobSubmissionStore submissions,
        IOptions<JobRuntimeOptions> options,
        QueueCatalog queueCatalog,
        CompletionBatcher? completionBatcher = null,
        KubeJobControlPlaneMetrics? metrics = null)
    {
        _sessions = sessions;
        _claims = claims;
        _completions = completions;
        _queries = queries;
        _submissions = submissions;
        _completionBatcher = completionBatcher;
        _options = options.Value;
        _queueCatalog = queueCatalog;
        _metrics = metrics;
    }

    public async ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            request = request with
            {
                Queues = _queueCatalog.NormalizeWorkerQueues(request.Queues),
                ConsumerGroup = request.ConsumerGroup?.Trim() ?? string.Empty,
                ExecutionLane = request.ExecutionLane?.Trim() ?? string.Empty
            };
        }
        catch (ArgumentException exception)
        {
            throw new ControlPlaneValidationException(
                "invalid_worker_queue",
                exception.Message);
        }

        ValidateRegistration(request);
        var session = await _sessions.RegisterAsync(request, cancellationToken);
        return new RegisterWorkerSessionResponse(
            session.WorkerId,
            session.SessionId,
            session.Epoch,
            session.StartedAt);
    }

    public ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken = default) =>
        _sessions.HeartbeatAsync(request, cancellationToken);

    public ValueTask<bool> CloseAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken = default) =>
        _sessions.CloseAsync(
            request.WorkerId,
            request.SessionId,
            request.SessionEpoch,
            cancellationToken);

    public async ValueTask<ClaimJobsResponse> ClaimAsync(
        ClaimJobsRequest request,
        CancellationToken cancellationToken = default)
    {
        if (request.RunIds is not null
            && request.RunIds.Any(string.IsNullOrWhiteSpace))
        {
            return new ClaimJobsResponse(Array.Empty<ClaimedJob>());
        }

        if (request.AvailableSlots <= 0)
        {
            return new ClaimJobsResponse(Array.Empty<ClaimedJob>());
        }

        var jobs = await _claims.ClaimAsync(
            request,
            _options.LeaseDuration,
            _options.MaxClaimBatchSize,
            cancellationToken);
        return new ClaimJobsResponse(jobs);
    }

    public async ValueTask<AdmitExecutionResponse> AdmitAsync(
        AdmitExecutionRequest request,
        CancellationToken cancellationToken = default)
    {
        if (request is null
            || string.IsNullOrWhiteSpace(request.WorkerId)
            || string.IsNullOrWhiteSpace(request.SessionId)
            || string.IsNullOrWhiteSpace(request.ConsumerGroup)
            || string.IsNullOrWhiteSpace(request.ExecutionLane)
            || string.IsNullOrWhiteSpace(request.RunId)
            || request.Queues is null
            || request.Capabilities is null
            || request.Queues.Any(string.IsNullOrWhiteSpace)
            || request.Capabilities.Any(string.IsNullOrWhiteSpace))
        {
            return new AdmitExecutionResponse(
                ExecutionAdmissionStatus.Rejected,
                Reason: "invalid_admission_request");
        }

        if (request.AvailableSlots <= 0)
        {
            return new AdmitExecutionResponse(
                ExecutionAdmissionStatus.Retry,
                Reason: "worker_capacity_exhausted");
        }

        var stopwatch = _metrics?.IsAdmissionDurationEnabled == true ? Stopwatch.StartNew() : null;
        var jobs = await _claims.ClaimAsync(
            new ClaimJobsRequest(
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                request.AvailableSlots,
                request.Queues,
                request.Capabilities,
                new[] { request.RunId },
                request.ConsumerGroup,
                request.ExecutionLane),
            _options.LeaseDuration,
            _options.MaxClaimBatchSize,
            cancellationToken);
        if (jobs.Count == 1)
        {
            var job = jobs[0];
            if (stopwatch is not null)
            {
                _metrics!.AdmissionCompleted(stopwatch.Elapsed, "admitted");
            }

            if (job.OrderingMode == ExecutionOrderingMode.KeyOrdered
                && _metrics is { IsOrderingWaitEnabled: true } orderingMetrics)
            {
                orderingMetrics.OrderingAdmitted(
                    DateTimeOffset.UtcNow - job.AvailableAt,
                    job.Queue);
            }

            return new AdmitExecutionResponse(
                ExecutionAdmissionStatus.Admitted,
                job);
        }

        // The normal BrokerDispatch path has already attempted the targeted
        // Claim. Only duplicate, terminal, not-found, or temporarily
        // unclaimable envelopes need the diagnostic read below. This removes
        // one PostgreSQL round-trip from every successfully admitted message.
        var run = await _queries.GetRunAsync(request.RunId, cancellationToken);
        var (status, reason) = ClassifyUnclaimed(
            run,
            request.ConsumerGroup,
            request.ExecutionLane,
            request.Queues,
            request.Capabilities);
        var response = new AdmitExecutionResponse(status, Reason: reason);

        if (stopwatch is not null)
        {
            _metrics!.AdmissionCompleted(stopwatch.Elapsed, response.Reason ?? response.Status.ToString());
        }

        return response;
    }

    /// <summary>
    /// Admits several broker-delivered envelopes in one claim transaction. All
    /// runs share the same worker session context; each run still passes the
    /// durable claim gate individually, so ordering and fencing semantics are
    /// identical to the per-envelope path. Results preserve input order.
    /// </summary>
    public async ValueTask<AdmitExecutionBatchResponse> AdmitBatchAsync(
        AdmitExecutionBatchRequest request,
        CancellationToken cancellationToken = default)
    {
        if (request is null
            || string.IsNullOrWhiteSpace(request.WorkerId)
            || string.IsNullOrWhiteSpace(request.SessionId)
            || string.IsNullOrWhiteSpace(request.ConsumerGroup)
            || string.IsNullOrWhiteSpace(request.ExecutionLane)
            || request.RunIds is null
            || request.Queues is null
            || request.Capabilities is null
            || request.Queues.Any(string.IsNullOrWhiteSpace)
            || request.Capabilities.Any(string.IsNullOrWhiteSpace))
        {
            return new AdmitExecutionBatchResponse(
                request?.RunIds is null
                    ? Array.Empty<AdmitExecutionResult>()
                    : request.RunIds
                        .Select(runId => new AdmitExecutionResult(
                            runId ?? string.Empty,
                            ExecutionAdmissionStatus.Rejected,
                            Reason: "invalid_admission_request"))
                        .ToArray());
        }

        var results = new AdmitExecutionResult[request.RunIds.Count];
        for (var index = 0; index < request.RunIds.Count; index++)
        {
            if (string.IsNullOrWhiteSpace(request.RunIds[index]))
            {
                results[index] = new AdmitExecutionResult(
                    request.RunIds[index] ?? string.Empty,
                    ExecutionAdmissionStatus.Rejected,
                    Reason: "invalid_admission_request");
            }
        }

        var validIndexes = Enumerable.Range(0, request.RunIds.Count)
            .Where(index => results[index] is null)
            .ToArray();
        if (validIndexes.Length == 0)
        {
            return new AdmitExecutionBatchResponse(results);
        }

        if (request.AvailableSlots <= 0)
        {
            foreach (var index in validIndexes)
            {
                results[index] = new AdmitExecutionResult(
                    request.RunIds[index],
                    ExecutionAdmissionStatus.Retry,
                    Reason: "worker_capacity_exhausted");
            }

            return new AdmitExecutionBatchResponse(results);
        }

        var stopwatch = _metrics?.IsAdmissionDurationEnabled == true ? Stopwatch.StartNew() : null;
        var jobs = await _claims.ClaimAsync(
            new ClaimJobsRequest(
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                request.AvailableSlots,
                request.Queues,
                request.Capabilities,
                validIndexes.Select(index => request.RunIds[index]).ToArray(),
                request.ConsumerGroup,
                request.ExecutionLane),
            _options.LeaseDuration,
            _options.MaxClaimBatchSize,
            cancellationToken);

        // A broker can redeliver the same Run more than once in one micro-batch.
        // Only the first envelope may own the newly created Attempt; subsequent
        // copies must be classified from durable state instead of becoming null
        // response slots or scheduling the same attempt twice.
        var firstIndexByRunId = validIndexes
            .GroupBy(index => request.RunIds[index], StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.Ordinal);

        foreach (var job in jobs)
        {
            if (firstIndexByRunId.TryGetValue(job.RunId, out var index))
            {
                results[index] = new AdmitExecutionResult(
                    job.RunId,
                    ExecutionAdmissionStatus.Admitted,
                    job);
            }
        }

        var unclaimedIndexes = validIndexes
            .Where(index => results[index] is null)
            .ToArray();

        if (unclaimedIndexes.Length > 0)
        {
            var runs = await _queries.GetRunsAsync(
                unclaimedIndexes.Select(index => request.RunIds[index]).ToArray(),
                cancellationToken);
            var runsById = runs.ToDictionary(run => run.Id, StringComparer.Ordinal);
            foreach (var index in unclaimedIndexes)
            {
                runsById.TryGetValue(request.RunIds[index], out var run);
                var (status, reason) = ClassifyUnclaimed(
                    run,
                    request.ConsumerGroup,
                    request.ExecutionLane,
                    request.Queues,
                    request.Capabilities);
                results[index] = new AdmitExecutionResult(request.RunIds[index], status, Reason: reason);
            }
        }

        if (stopwatch is not null)
        {
            _metrics!.AdmissionCompleted(
                stopwatch.Elapsed,
                results.Any(result => result.Status == ExecutionAdmissionStatus.Admitted)
                    ? "admitted"
                    : results.FirstOrDefault()?.Reason ?? "batch");
        }

        return new AdmitExecutionBatchResponse(results);
    }

    private static (ExecutionAdmissionStatus Status, string? Reason) ClassifyUnclaimed(
        JobRunRecord? run,
        string consumerGroup,
        string executionLane,
        IReadOnlyList<string> queues,
        IReadOnlyList<string> capabilities)
    {
        if (run is null)
        {
            // The envelope is for a Run that has been hard-deleted; the broker
            // will redeliver until the run is found. Retry so the broker keeps
            // the message until either the Run is recreated or the broker
            // delivery limit is reached and we reconcile via the outbox.
            return (ExecutionAdmissionStatus.Retry, "run_not_found");
        }

        if (run.Phase is JobPhase.Succeeded
            or JobPhase.Failed
            or JobPhase.Canceled
            or JobPhase.Dead
            || run.CancelRequested)
        {
            return (ExecutionAdmissionStatus.AlreadyTerminal, "run_already_terminal");
        }

        if (run.Phase == JobPhase.Running)
        {
            // Another worker holds the lease; the broker should redeliver to
            // a different worker once the lease expires or the run completes.
            // Rejecting here would silently drop the envelope.
            return (ExecutionAdmissionStatus.Retry, "run_already_running");
        }

        if (!string.Equals(consumerGroup, run.ConsumerGroup, StringComparison.Ordinal)
            || !string.Equals(executionLane, run.ExecutionLane, StringComparison.Ordinal)
            || !queues.Contains(run.Queue, StringComparer.Ordinal)
            || !capabilities.Contains(run.JobKey, StringComparer.Ordinal))
        {
            // The broker misrouted this envelope to a worker that cannot run
            // it. Retry so a different worker (with the right queue/capability)
            // can pick it up after the broker rebalances. Reject would lose it.
            return string.Equals(consumerGroup, run.ConsumerGroup, StringComparison.Ordinal)
                   && string.Equals(executionLane, run.ExecutionLane, StringComparison.Ordinal)
                ? (ExecutionAdmissionStatus.Retry, "worker_not_capable")
                : (ExecutionAdmissionStatus.Retry, "worker_profile_mismatch");
        }

        return (ExecutionAdmissionStatus.Retry, "run_not_claimable");
    }

    public async ValueTask<RenewLeasesResponse> RenewLeasesAsync(
        RenewLeasesRequest request,
        CancellationToken cancellationToken = default)
    {
        var attempts = await _claims.RenewLeasesAsync(
            request,
            _options.LeaseDuration,
            cancellationToken);
        return new RenewLeasesResponse(attempts);
    }

    public ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken = default) =>
        _completionBatcher is null
            ? _completions.CompleteAsync(request, _options.RetryPolicy, cancellationToken)
            : _completionBatcher.EnqueueAsync(request, cancellationToken);

    public ValueTask<bool> RequeueExecutionAsync(
        RequeueExecutionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RunId);
        return _submissions.RequeueWorkAvailableAsync(
            request.RunId,
            request.AvailableAt,
            cancellationToken);
    }

    private static void ValidateRegistration(RegisterWorkerSessionRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (string.IsNullOrWhiteSpace(request.WorkerId)
            || string.IsNullOrWhiteSpace(request.SessionId)
            || string.IsNullOrWhiteSpace(request.ConsumerGroup)
            || string.IsNullOrWhiteSpace(request.ExecutionLane)
            || request.MaxConcurrency < 1
            || request.Queues is null
            || request.Queues.Count == 0
            || request.Queues.Any(string.IsNullOrWhiteSpace)
            || request.Capabilities is null
            || request.Capabilities.Count == 0
            || request.Capabilities.Any(string.IsNullOrWhiteSpace)
            || request.Labels is null)
        {
            throw new ControlPlaneValidationException(
                "invalid_worker_registration",
                "Worker identity, positive capacity, queues, capabilities, and labels are required.");
        }
    }
}
