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
/// Controls the execution ordering contract for a logical queue. Ordering is
/// enforced by the control plane for PostgresManaged execution.
/// </summary>
public enum ExecutionOrderingMode
{
    Parallel = 0,
    KeyOrdered = 1,
    StrictFifo = 2
}

/// <summary>
/// Legacy PostgresManaged delivery metadata retained for schema compatibility
/// while BrokerNative queues use QueueRuntimeMode plus IMessageTransport.
/// Pull is the only supported managed execution authority; any stale transport
/// metadata is discarded at construction time.
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
        this.TransportId = Profile == ExecutionDeliveryProfile.Pull ? null : TransportId;
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
/// Legacy V2 carrier for a previously accepted managed Run. New broker-owned
/// execution uses BrokerNativeJobMessage instead and never performs admission.
/// </summary>
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
/// Legacy V2 execution transport contract. New transports implement
/// IMessageTransportPublisher for BrokerNative execution.
/// </summary>
public interface IExecutionTransport
{
    string TransportId { get; }

    ValueTask PublishAsync(
        ExecutionEnvelope envelope,
        CancellationToken cancellationToken);

    IReadOnlyList<string> ResolvePhysicalQueueNames(string logicalQueue)
        => Array.Empty<string>();
}
