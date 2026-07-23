using KubeJob.Core.Domain;
using KubeJob.Core.Options;

namespace KubeJob.Core.Interfaces;

public interface IKubeJobClient
{
    Task<JobSubmissionResult> EnqueueAsync<TPayload>(
        string jobName,
        TPayload payload,
        JobEnqueueOptions? options = null,
        CancellationToken cancellationToken = default);

    Task<bool> CancelRunAsync(
        string runId,
        string reason = "Canceled by user",
        CancellationToken cancellationToken = default);

    Task<int> CancelBatchAsync(
        string batchId,
        string reason = "Canceled by user",
        CancellationToken cancellationToken = default);
}
