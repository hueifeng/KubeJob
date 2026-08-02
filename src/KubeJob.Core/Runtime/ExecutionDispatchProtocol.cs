namespace KubeJob.Core.Runtime;

/// <summary>
/// Platform-owned delivery choices for a logical Queue. Applications do not
/// select this value when submitting a Run.
/// </summary>
public enum ExecutionDeliveryProfile
{
    Pull = 0,
    BrokerDispatch = 1
}

/// <summary>
/// Controls the execution ordering contract for a logical queue.  Ordering is
/// enforced by the control plane, rather than by a broker-specific consumer
/// setting, so it survives redelivery and worker failover.
/// </summary>
public enum ExecutionOrderingMode
{
    Parallel = 0,
    KeyOrdered = 1,
    /// <summary>
    /// Strict global FIFO: the entire queue (or lane) is processed one
    /// message at a time. The next message is NEVER dispatched while the
    /// current message is inflight. Equivalent to prefetch=1 on every
    /// consumer for this queue.
    /// PostgreSQL KeyOrdered gate logic also applies: the control plane
    /// verifies OrderingSequence monotonicity even in StrictFifo mode.
    /// </summary>
    StrictFifo = 2
}

/// <summary>
/// A durable, transport-neutral destination selected by deployment policy when
/// a Run is created. An execution lane is a logical Worker eligibility and
/// isolation boundary; it is not a RabbitMQ, Kafka, or RocketMQ group name.
/// </summary>
public sealed record DeliveryTarget
{
    public ExecutionDeliveryProfile Profile { get; init; }
    public string ExecutionLane { get; init; }
    public string ConsumerGroup { get; init; }
    public string? TransportId { get; init; }
    public ExecutionOrderingMode OrderingMode { get; init; }

    public DeliveryTarget(
        ExecutionDeliveryProfile Profile,
        string ExecutionLane,
        string? TransportId,
        string ConsumerGroup,
        ExecutionOrderingMode OrderingMode = ExecutionOrderingMode.Parallel)
    {
        this.Profile = Profile;
        this.ExecutionLane = ExecutionLane;
        this.ConsumerGroup = ConsumerGroup;
        this.TransportId = TransportId;
        this.OrderingMode = OrderingMode;
    }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ExecutionLane))
        {
            throw new InvalidOperationException("KubeJob execution lane is required.");
        }

        if (string.IsNullOrWhiteSpace(ConsumerGroup))
        {
            throw new InvalidOperationException("KubeJob consumer group is required.");
        }

        if (!Enum.IsDefined(OrderingMode))
        {
            throw new InvalidOperationException("KubeJob execution ordering mode is invalid.");
        }

        if (Profile == ExecutionDeliveryProfile.Pull)
        {
            if (!string.IsNullOrWhiteSpace(TransportId))
            {
                throw new InvalidOperationException("Pull delivery cannot specify a transport ID.");
            }

            return;
        }

        if (Profile != ExecutionDeliveryProfile.BrokerDispatch
            || string.IsNullOrWhiteSpace(TransportId))
        {
            throw new InvalidOperationException(
                "Broker dispatch requires a supported delivery profile and transport ID.");
        }
    }
}

/// <summary>
/// A transport-neutral carrier for an already accepted logical Run. The
/// envelope is not execution authority; the Worker still needs admission and
/// a valid lease from the control plane.
/// </summary>
/// <remarks>
/// <see cref="PartitionKey"/> carries the run's ConcurrencyKey so transport
/// adapters can hash it to a fixed-N physical lane queue; a null value
/// resolves to lane 0.
/// </remarks>
public sealed record ExecutionEnvelope
{
    public int SchemaVersion { get; init; }
    public required string EventId { get; init; }
    public required string Queue { get; init; }
    public required string ExecutionLane { get; init; }
    public required string ConsumerGroup { get; init; }
    public required string RunId { get; init; }
    public string? PartitionKey { get; init; }

    public const int CurrentSchemaVersion = 3;

    public static ExecutionEnvelope FromWorkAvailableSignal(
        WorkAvailableSignal signal)
    {
        ArgumentNullException.ThrowIfNull(signal);
        return new ExecutionEnvelope
        {
            SchemaVersion = CurrentSchemaVersion,
            EventId = signal.EventId,
            Queue = signal.Queue,
            ExecutionLane = signal.ExecutionLane,
            RunId = signal.RunId,
            ConsumerGroup = signal.ConsumerGroup,
            PartitionKey = signal.PartitionKey
        };
    }
}

/// <summary>
/// Publishes execution envelopes for one named transport. Adapter packages own
/// physical topics, queues, confirms, commits, and retry mechanics; they never
/// own KubeJob Run, Attempt, lease, or completion state.
/// </summary>
public interface IExecutionTransport
{
    string TransportId { get; }

    ValueTask PublishAsync(
        ExecutionEnvelope envelope,
        CancellationToken cancellationToken);

    /// <summary>
    /// Resolves the physical queue names a logical queue maps to on this
    /// transport (for example one dispatch queue per lane plus shared retry and
    /// dead-letter queues). Used by operational surfaces such as the Dashboard
    /// queue inventory. Adapters without a physical queue concept return an
    /// empty list.
    /// </summary>
    IReadOnlyList<string> ResolvePhysicalQueueNames(string logicalQueue)
        => Array.Empty<string>();
}
