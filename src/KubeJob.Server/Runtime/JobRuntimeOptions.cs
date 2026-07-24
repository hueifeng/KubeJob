namespace KubeJob.Server.Runtime;

public sealed class JobRuntimeOptions
{
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan LeaseReaperInterval { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan OutboxPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan OutboxClaimDuration { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan OutboxFailureDelay { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan SchedulePollInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan ScheduleClaimDuration { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan ScheduleFailureDelay { get; set; } = TimeSpan.FromSeconds(5);

    public int MaxClaimBatchSize { get; set; } = 32;

    public int LeaseReaperBatchSize { get; set; } = 256;

    public int OutboxBatchSize { get; set; } = 128;

    public int ScheduleBatchSize { get; set; } = 128;

    public void Validate()
    {
        if (LeaseDuration <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("LeaseDuration must be positive.");
        }

        if (RetryDelay < TimeSpan.Zero)
        {
            throw new InvalidOperationException("RetryDelay cannot be negative.");
        }

        if (LeaseReaperInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("LeaseReaperInterval must be positive.");
        }

        if (OutboxPollInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("OutboxPollInterval must be positive.");
        }

        if (OutboxClaimDuration <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("OutboxClaimDuration must be positive.");
        }

        if (OutboxFailureDelay < TimeSpan.Zero)
        {
            throw new InvalidOperationException("OutboxFailureDelay cannot be negative.");
        }

        if (SchedulePollInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("SchedulePollInterval must be positive.");
        }

        if (ScheduleClaimDuration <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("ScheduleClaimDuration must be positive.");
        }

        if (ScheduleFailureDelay < TimeSpan.Zero)
        {
            throw new InvalidOperationException("ScheduleFailureDelay cannot be negative.");
        }

        if (MaxClaimBatchSize is < 1 or > 1024)
        {
            throw new InvalidOperationException("MaxClaimBatchSize must be between 1 and 1024.");
        }

        if (LeaseReaperBatchSize is < 1 or > 10_000)
        {
            throw new InvalidOperationException("LeaseReaperBatchSize must be between 1 and 10000.");
        }

        if (OutboxBatchSize is < 1 or > 10_000)
        {
            throw new InvalidOperationException("OutboxBatchSize must be between 1 and 10000.");
        }

        if (ScheduleBatchSize is < 1 or > 10_000)
        {
            throw new InvalidOperationException("ScheduleBatchSize must be between 1 and 10000.");
        }
    }
}
