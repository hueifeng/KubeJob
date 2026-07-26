using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Executes the worker protocol directly against the configured stores.
/// Unified hosting therefore has the same attempt/lease semantics as remote
/// workers without routing through localhost HTTP.
/// </summary>
public sealed class InProcessWorkerRuntimeClient : IWorkerRuntimeClient
{
    private readonly WorkerControlPlane _controlPlane;

    public InProcessWorkerRuntimeClient(WorkerControlPlane controlPlane)
    {
        _controlPlane = controlPlane;
    }

    public async ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        return await _controlPlane.RegisterAsync(request, cancellationToken);
    }

    public ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken) =>
        _controlPlane.HeartbeatAsync(request, cancellationToken);

    public ValueTask<bool> CloseAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken) =>
        _controlPlane.CloseAsync(request, cancellationToken);

    public async ValueTask<ClaimJobsResponse> ClaimAsync(
        ClaimJobsRequest request,
        CancellationToken cancellationToken)
    {
        return await _controlPlane.ClaimAsync(request, cancellationToken);
    }

    public ValueTask<AdmitExecutionResponse> AdmitAsync(
        AdmitExecutionRequest request,
        CancellationToken cancellationToken) =>
        _controlPlane.AdmitAsync(request, cancellationToken);

    public async ValueTask<RenewLeasesResponse> RenewLeasesAsync(
        RenewLeasesRequest request,
        CancellationToken cancellationToken)
    {
        return await _controlPlane.RenewLeasesAsync(request, cancellationToken);
    }

    public ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken) =>
        _controlPlane.CompleteAsync(request, cancellationToken);

    public ValueTask<bool> RequeueExecutionAsync(
        RequeueExecutionRequest request,
        CancellationToken cancellationToken) =>
        _controlPlane.RequeueExecutionAsync(request, cancellationToken);
}
