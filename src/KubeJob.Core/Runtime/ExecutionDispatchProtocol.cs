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
/// A durable, transport-neutral destination selected by deployment policy when
/// a Run is created. An execution lane is a logical Worker eligibility and
/// isolation boundary; it is not a RabbitMQ, Kafka, or RocketMQ group name.
/// </summary>
public sealed record DeliveryTarget(
    ExecutionDeliveryProfile Profile,
    string ExecutionLane,
    string? TransportId)
{
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ExecutionLane))
        {
            throw new InvalidOperationException("KubeJob execution lane is required.");
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
public sealed record ExecutionEnvelope(
    int SchemaVersion,
    string EventId,
    string Queue,
    string ExecutionLane,
    string RunId)
{
    public const int CurrentSchemaVersion = 2;

    public static ExecutionEnvelope FromWorkAvailableSignal(
        WorkAvailableSignal signal)
    {
        ArgumentNullException.ThrowIfNull(signal);
        return new ExecutionEnvelope(
            CurrentSchemaVersion,
            signal.EventId,
            signal.Queue,
            signal.ExecutionLane,
            signal.RunId);
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
}
