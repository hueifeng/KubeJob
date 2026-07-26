namespace KubeJob.Core.Runtime;

/// <summary>
/// Worker-facing control-plane protocol. Hosting packages provide remote HTTP
/// and in-process implementations without exposing the transport to handlers.
/// </summary>
public interface IWorkerRuntimeClient
{
    ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken);

    ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken);

    ValueTask<bool> CloseAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken);

    ValueTask<ClaimJobsResponse> ClaimAsync(
        ClaimJobsRequest request,
        CancellationToken cancellationToken);

    ValueTask<AdmitExecutionResponse> AdmitAsync(
        AdmitExecutionRequest request,
        CancellationToken cancellationToken);

    ValueTask<RenewLeasesResponse> RenewLeasesAsync(
        RenewLeasesRequest request,
        CancellationToken cancellationToken);

    ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken);
}
