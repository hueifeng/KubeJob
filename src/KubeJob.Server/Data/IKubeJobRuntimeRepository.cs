using KubeJob.Core.Domain;
using KubeJob.Core.Dtos;

namespace KubeJob.Server.Data;

public interface IKubeJobRuntimeRepository
{
    Task<long> RegisterWorkerSessionAsync(RegisterWorkerSessionRequest request, string labelsJson, CancellationToken cancellationToken);
    Task<IReadOnlyList<JobLease>> ClaimRunsAsync(string workerId, string sessionId, long sessionEpoch,
        IReadOnlyList<string> queueNames, int limit, TimeSpan leaseDuration, CancellationToken cancellationToken);
    Task<RenewLeasesResponse> RenewLeasesAsync(RenewLeasesRequest request, TimeSpan leaseDuration,
        CancellationToken cancellationToken);
    Task<bool> TryCompleteRunAsync(CompleteRunRequest request, CancellationToken cancellationToken);
    Task<int> RequeueExpiredLeasesAsync(int limit, CancellationToken cancellationToken);
    Task<int> FinalizeOrphanedPinnedRunsAsync(TimeSpan heartbeatTimeout, int limit,
        CancellationToken cancellationToken);
    Task<int> CleanupOrphanedBatchMetadataAsync(TimeSpan idempotencyRetention, int limit,
        CancellationToken cancellationToken);
}
