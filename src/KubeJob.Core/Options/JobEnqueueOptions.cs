namespace KubeJob.Core.Options;

public sealed class JobEnqueueOptions
{
    public string QueueName { get; set; } = string.Empty;
    public int? Priority { get; set; }
    public DateTimeOffset? AvailableAt { get; set; }
    public string IdempotencyKey { get; set; } = string.Empty;
    public int PayloadSchemaVersion { get; set; } = 1;
}
