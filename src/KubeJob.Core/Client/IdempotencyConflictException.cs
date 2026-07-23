namespace KubeJob.Core.Client;

/// <summary>
/// Raised when an idempotency key already belongs to a different logical job submission.
/// </summary>
public sealed class IdempotencyConflictException : InvalidOperationException
{
    public IdempotencyConflictException(
        string idempotencyKey,
        string existingJobId)
        : base($"Idempotency key '{idempotencyKey}' is already associated with a different job submission.")
    {
        IdempotencyKey = idempotencyKey;
        ExistingJobId = existingJobId;
    }

    public string IdempotencyKey { get; }

    public string ExistingJobId { get; }
}
