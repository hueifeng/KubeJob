using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.ControlPlane;

/// <summary>
/// Owns PostgresManaged worker-session, claim, lease, and completion
/// orchestration. BrokerNative consumers do not call this control plane in the
/// execution hot path.
/// </summary>
public sealed class WorkerControlPlane
{
    private readonly IWorkerSessionStore _sessions;
    private readonly IJobClaimStore _claims;
    private readonly IJobCompletionStore _completions;
    private readonly IJobSubmissionStore _submissions;
    private readonly CompletionBatcher? _completionBatcher;
    private readonly JobRuntimeOptions _options;
    private readonly QueueCatalog _queueCatalog;

    public WorkerControlPlane(
        IWorkerSessionStore sessions,
        IJobClaimStore claims,
        IJobCompletionStore completions,
        IJobSubmissionStore submissions,
        IOptions<JobRuntimeOptions> options,
        QueueCatalog queueCatalog,
        CompletionBatcher? completionBatcher = null)
    {
        _sessions = sessions;
        _claims = claims;
        _completions = completions;
        _submissions = submissions;
        _completionBatcher = completionBatcher;
        _options = options.Value;
        _queueCatalog = queueCatalog;
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
