namespace KubeJob.Core.Runtime;

/// <summary>
/// PostgresManaged worker-facing control-plane protocol. BrokerNative workers
/// consume directly from their transport and do not use this client.
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

    ValueTask<RenewLeasesResponse> RenewLeasesAsync(
        RenewLeasesRequest request,
        CancellationToken cancellationToken);

    ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken);

    ValueTask<bool> RequeueExecutionAsync(
        RequeueExecutionRequest request,
        CancellationToken cancellationToken);
}
