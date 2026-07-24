using KubeJob.Core.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Executes the worker protocol directly against the configured stores.
/// Unified hosting therefore has the same attempt/lease semantics as remote
/// workers without routing through localhost HTTP.
/// </summary>
public sealed class InProcessWorkerRuntimeClient : IWorkerRuntimeClient
{
    private readonly IWorkerSessionStore _sessions;
    private readonly IJobClaimStore _claims;
    private readonly IJobCompletionStore _completions;
    private readonly JobRuntimeOptions _options;

    public InProcessWorkerRuntimeClient(
        IWorkerSessionStore sessions,
        IJobClaimStore claims,
        IJobCompletionStore completions,
        IOptions<JobRuntimeOptions> options)
    {
        _sessions = sessions;
        _claims = claims;
        _completions = completions;
        _options = options.Value;
    }

    public async ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        var session = await _sessions.RegisterAsync(request, cancellationToken);
        return new RegisterWorkerSessionResponse(
            session.WorkerId,
            session.SessionId,
            session.Epoch,
            session.StartedAt);
    }

    public ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken) =>
        _sessions.HeartbeatAsync(request, cancellationToken);

    public ValueTask<bool> CloseAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken) =>
        _sessions.CloseAsync(
            request.WorkerId,
            request.SessionId,
            request.SessionEpoch,
            cancellationToken);

    public async ValueTask<ClaimJobsResponse> ClaimAsync(
        ClaimJobsRequest request,
        CancellationToken cancellationToken)
    {
        var jobs = await _claims.ClaimAsync(
            request,
            _options.LeaseDuration,
            _options.MaxClaimBatchSize,
            cancellationToken);
        return new ClaimJobsResponse(jobs);
    }

    public async ValueTask<RenewLeasesResponse> RenewLeasesAsync(
        RenewLeasesRequest request,
        CancellationToken cancellationToken)
    {
        var attempts = await _claims.RenewLeasesAsync(
            request,
            _options.LeaseDuration,
            cancellationToken);
        return new RenewLeasesResponse(attempts);
    }

    public ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken) =>
        _completions.CompleteAsync(
            request,
            _options.RetryDelay,
            cancellationToken);
}
