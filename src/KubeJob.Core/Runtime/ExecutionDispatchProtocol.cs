namespace KubeJob.Core.Runtime;

/// <summary>
/// Durable managed delivery profile. PostgresManaged is PostgreSQL-authoritative;
/// BrokerNative delivery is modeled separately by QueueRuntimeMode and transport
/// contracts, not as a managed Run delivery profile.
/// </summary>
public enum ExecutionDeliveryProfile
{
    Pull = 0
}

/// <summary>
/// Ordering contract for PostgresManaged queues. BrokerNative ordering is a
/// transport-native concern and does not use the managed claim gate.
/// </summary>
public enum ExecutionOrderingMode
{
    Parallel = 0,
    KeyOrdered = 1,
    StrictFifo = 2
}

/// <summary>
/// PostgresManaged worker policy stamped onto durable Runs. TransportId is
/// retained as a null compatibility field while the existing storage schema is
/// migrated; new managed Runs never populate it.
/// </summary>
public sealed record DeliveryTarget
{
    public ExecutionDeliveryProfile Profile { get; init; } = ExecutionDeliveryProfile.Pull;
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
        if (Profile != ExecutionDeliveryProfile.Pull)
        {
            throw new InvalidOperationException("PostgresManaged only supports Pull delivery.");
        }

        this.Profile = ExecutionDeliveryProfile.Pull;
        this.ExecutionLane = ExecutionLane;
        this.ConsumerGroup = ConsumerGroup;
        this.TransportId = null;
        this.OrderingMode = OrderingMode;
    }

    public void Validate()
    {
        if (Profile != ExecutionDeliveryProfile.Pull)
        {
            throw new InvalidOperationException("PostgresManaged only supports Pull delivery.");
        }

        if (string.IsNullOrWhiteSpace(ExecutionLane))
        {
            throw new InvalidOperationException("KubeJob managed execution lane is required.");
        }

        if (string.IsNullOrWhiteSpace(ConsumerGroup))
        {
            throw new InvalidOperationException("KubeJob managed consumer group is required.");
        }

        if (!Enum.IsDefined(OrderingMode))
        {
            throw new InvalidOperationException("KubeJob managed ordering mode is invalid.");
        }
    }
}
