using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Executes the PostgresManaged worker protocol directly against the control
/// plane for unified hosting without localhost HTTP.
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
        CancellationToken cancellationToken) =>
        await _controlPlane.RegisterAsync(request, cancellationToken);

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
        CancellationToken cancellationToken) =>
        await _controlPlane.ClaimAsync(request, cancellationToken);

    public async ValueTask<RenewLeasesResponse> RenewLeasesAsync(
        RenewLeasesRequest request,
        CancellationToken cancellationToken) =>
        await _controlPlane.RenewLeasesAsync(request, cancellationToken);

    public ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken) =>
        _controlPlane.CompleteAsync(request, cancellationToken);

    public ValueTask<bool> RequeueExecutionAsync(
        RequeueExecutionRequest request,
        CancellationToken cancellationToken) =>
        _controlPlane.RequeueExecutionAsync(request, cancellationToken);
}
