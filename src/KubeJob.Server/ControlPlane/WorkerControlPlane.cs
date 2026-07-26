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

    public WorkerControlPlane(
        IWorkerSessionStore sessions,
        IJobClaimStore claims,
        IJobCompletionStore completions,
        IJobQueryStore queries,
        IJobSubmissionStore submissions,
        IOptions<JobRuntimeOptions> options,
        CompletionBatcher? completionBatcher = null)
    {
        _sessions = sessions;
        _claims = claims;
        _completions = completions;
        _queries = queries;
        _submissions = submissions;
        _completionBatcher = completionBatcher;
        _options = options.Value;
    }

    public async ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken = default)
    {
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

        var jobs = await _claims.ClaimAsync(
            new ClaimJobsRequest(
                request.WorkerId,
                request.SessionId,
                request.SessionEpoch,
                request.AvailableSlots,
                request.Queues,
                request.Capabilities,
                new[] { request.RunId }),
            _options.LeaseDuration,
            _options.MaxClaimBatchSize,
            cancellationToken);
        if (jobs.Count == 1)
        {
            return new AdmitExecutionResponse(
                ExecutionAdmissionStatus.Admitted,
                jobs[0]);
        }

        // The normal BrokerDispatch path has already attempted the targeted
        // Claim. Only duplicate, terminal, not-found, or temporarily
        // unclaimable envelopes need the diagnostic read below. This removes
        // one PostgreSQL round-trip from every successfully admitted message.
        var run = await _queries.GetRunAsync(request.RunId, cancellationToken);
        if (run is null)
        {
            return new AdmitExecutionResponse(
                ExecutionAdmissionStatus.NotFound,
                Reason: "run_not_found");
        }

        if (run.Phase is JobPhase.Succeeded
            or JobPhase.Failed
            or JobPhase.Canceled
            or JobPhase.Dead
            || run.CancelRequested)
        {
            return new AdmitExecutionResponse(
                ExecutionAdmissionStatus.AlreadyTerminal,
                Reason: "run_already_terminal");
        }

        if (run.Phase == JobPhase.Running)
        {
            return new AdmitExecutionResponse(
                ExecutionAdmissionStatus.AlreadyTerminal,
                Reason: "run_already_running");
        }

        if (!request.Queues.Contains(run.Queue, StringComparer.Ordinal)
            || !request.Capabilities.Contains(run.JobKey, StringComparer.Ordinal))
        {
            return new AdmitExecutionResponse(
                ExecutionAdmissionStatus.Rejected,
                Reason: "worker_not_capable");
        }

        return new AdmitExecutionResponse(
            ExecutionAdmissionStatus.Retry,
            Reason: "run_not_claimable");
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
            ? _completions.CompleteAsync(request, _options.RetryDelay, cancellationToken)
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
