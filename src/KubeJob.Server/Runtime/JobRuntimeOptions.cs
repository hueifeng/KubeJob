using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public sealed class JobRuntimeOptions
{
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    public RetryPolicy RetryPolicy { get; set; } =
        new(BackoffStrategy.Fixed, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

    public TimeSpan LeaseReaperInterval { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan OutboxPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan OutboxClaimDuration { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan OutboxFailureDelay { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan SchedulePollInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan ScheduleClaimDuration { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan ScheduleFailureDelay { get; set; } = TimeSpan.FromSeconds(5);

    public int MaxClaimBatchSize { get; set; } = 32;

    public int LeaseReaperBatchSize { get; set; } = 256;

    public int OutboxBatchSize { get; set; } = 256;

    public int OutboxPublishConcurrency { get; set; } = 4;

    public int CompletionBatchSize { get; set; } = 32;

    /// <summary>
    /// Number of independent completion shards. When > 1, completion requests
    /// are routed to a shard by <c>RunId.GetHashCode() % ShardCount</c>. Each
    /// shard has its own bounded channel, micro-batch, and database batch
    /// submission, eliminating the single-batcher hot spot under high worker
    /// concurrency. Default 4 (Item 6: CompletionBatcher sharding).
    /// </summary>
    public int CompletionBatcherShardCount { get; set; } = 4;

    public TimeSpan CompletionFlushInterval { get; set; } = TimeSpan.FromMilliseconds(2);

    public int ScheduleBatchSize { get; set; } = 128;

    /// <summary>How many claimed schedules the reconciler fires concurrently per iteration.</summary>
    public int ScheduleReconcileConcurrency { get; set; } = 4;

    /// Maximum UTF-8 encoded payload size accepted at the control-plane boundary.
    public int MaxPayloadBytes { get; set; } = 1_048_576;

    /// <summary>How long successfully published Outbox rows remain for diagnostics.</summary>
    public TimeSpan PublishedOutboxRetention { get; set; } = TimeSpan.FromDays(1);

    /// <summary>
    /// Retention for terminal runs without idempotency or schedule identity.
    /// Keyed terminal history is retained by design: the idempotency key must
    /// never be reused while its historical Run still exists.
    /// </summary>
    public TimeSpan UnkeyedTerminalRetention { get; set; } = TimeSpan.FromDays(7);

    public TimeSpan RetentionPollInterval { get; set; } = TimeSpan.FromMinutes(1);

    public int RetentionBatchSize { get; set; } = 1_000;

    /// <summary>
    /// Period at which the control-plane refreshes its cached KeyOrdered
    /// ordering backlog snapshot. The cache backs the observable gauges, so a
    /// metrics scrape returns the cached value and never hits the database.
    /// Too small a value increases query load; too large delays backlog
    /// detection. Default 5s.
    /// </summary>
    public TimeSpan OrderingBacklogRefreshInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// Flag for broker-accelerated cancellation of BrokerDispatch runs.
    /// Submission always writes <c>work-available</c> outbox rows; the
    /// <c>OutboxPublisherService</c> converts them to an
    /// <see cref="KubeJob.Core.Runtime.ExecutionEnvelope"/> at publish time for
    /// queues whose delivery profile is
    /// <see cref="KubeJob.Core.Runtime.ExecutionDeliveryProfile.BrokerDispatch"/>,
    /// so this flag does not change submission outbox shape.
    /// When this flag is <c>true</c>, cancelling a BrokerDispatch-profile Run
    /// also writes a <c>cancel</c> outbox row so a registered
    /// <c>ICancelPublisher</c> can fan out a low-latency cancel signal to
    /// workers; when <c>false</c>, cancel only sets <c>CancelRequested</c> and
    /// relies on the lease reaper / renewal loop as the correctness fallback.
    /// Requires <c>RabbitMqNotificationExtensions.UseRabbitMqKubeJobExecutionDispatcher</c>
    /// and an <c>ICancelPublisher</c> implementation for the cancel path to
    /// function; hosts that stay on <c>Pull</c> and skip the RabbitMQ execution
    /// extensions can set this back to <c>false</c>. Default is <c>true</c>,
    /// matching <see cref="KubeJob.Server.Runtime.QueueDeliveryOptions.DefaultProfile"/>
    /// defaulting to <see cref="KubeJob.Core.Runtime.ExecutionDeliveryProfile.BrokerDispatch"/>.
    /// </summary>
    public bool BrokerCancelPropagationEnabled { get; set; } = true;

    public void Validate()
    {
        if (LeaseDuration <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("LeaseDuration must be positive.");
        }

        RetryPolicy.Validate();

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

        if (OutboxPublishConcurrency is < 1 or > 32)
        {
            throw new InvalidOperationException("OutboxPublishConcurrency must be between 1 and 32.");
        }

        if (CompletionBatchSize is < 1 or > 1_024)
        {
            throw new InvalidOperationException("CompletionBatchSize must be between 1 and 1024.");
        }

        if (CompletionBatcherShardCount is < 1 or > 64)
        {
            throw new InvalidOperationException("CompletionBatcherShardCount must be between 1 and 64.");
        }

        if (CompletionFlushInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("CompletionFlushInterval must be positive.");
        }

        if (ScheduleBatchSize is < 1 or > 10_000)
        {
            throw new InvalidOperationException("ScheduleBatchSize must be between 1 and 10000.");
        }

        if (ScheduleReconcileConcurrency is < 1 or > 64)
        {
            throw new InvalidOperationException("ScheduleReconcileConcurrency must be between 1 and 64.");
        }

        if (MaxPayloadBytes is < 1 or > 16 * 1024 * 1024)
        {
            throw new InvalidOperationException("MaxPayloadBytes must be between 1 byte and 16 MiB.");
        }

        if (PublishedOutboxRetention < TimeSpan.Zero)
        {
            throw new InvalidOperationException("PublishedOutboxRetention cannot be negative.");
        }

        if (UnkeyedTerminalRetention < TimeSpan.Zero)
        {
            throw new InvalidOperationException("UnkeyedTerminalRetention cannot be negative.");
        }

        if (RetentionPollInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("RetentionPollInterval must be positive.");
        }

        if (RetentionBatchSize is < 1 or > 10_000)
        {
            throw new InvalidOperationException("RetentionBatchSize must be between 1 and 10000.");
        }

        if (OrderingBacklogRefreshInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("OrderingBacklogRefreshInterval must be positive.");
        }
    }
}
