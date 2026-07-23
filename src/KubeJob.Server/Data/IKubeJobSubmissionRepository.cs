using KubeJob.Core.Domain;

namespace KubeJob.Server.Data;

public sealed class JobSubmissionCommand
{
    public string JobName { get; init; } = string.Empty;
    public byte[] PayloadUtf8 { get; init; } = Array.Empty<byte>();
    public byte[] PayloadHash { get; init; } = Array.Empty<byte>();
    public int PayloadSchemaVersion { get; init; } = 1;
    public string QueueName { get; init; } = string.Empty;
    public int? Priority { get; init; }
    public string IdempotencyKey { get; init; } = string.Empty;
    public DateTimeOffset? AvailableAt { get; init; }
}

public interface IKubeJobSubmissionRepository
{
    Task<JobSubmissionResult> SubmitAsync(JobSubmissionCommand command, CancellationToken cancellationToken);
    Task<bool> CancelRunAsync(string runId, string reason, CancellationToken cancellationToken);
    Task<int> CancelBatchAsync(string batchId, string reason, CancellationToken cancellationToken);
}
