namespace KubeJob.Core.Domain;

public sealed class JobSubmissionResult
{
    public string BatchId { get; init; } = string.Empty;
    public IReadOnlyList<string> RunIds { get; init; } = Array.Empty<string>();
    public bool IsDuplicate { get; init; }
}
