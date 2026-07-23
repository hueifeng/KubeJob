using KubeJob.Core.Dtos;

namespace KubeJob.Worker.Runtime;

public interface IKubeJobRuntimeClient
{
    Task<RegisterWorkerSessionResponse> RegisterAsync(RegisterWorkerSessionRequest request, CancellationToken cancellationToken);
    Task<ClaimRunsResponse> ClaimAsync(ClaimRunsRequest request, CancellationToken cancellationToken);
    Task<RenewLeasesResponse> RenewAsync(RenewLeasesRequest request, CancellationToken cancellationToken);
    Task<bool> CompleteAsync(CompleteRunRequest request, CancellationToken cancellationToken);
}
