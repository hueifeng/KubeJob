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
/// A transport-neutral carrier for an already accepted logical Run. The
/// envelope is not execution authority; the Worker still needs admission and
/// a valid lease from the control plane.
/// </summary>
public sealed record ExecutionEnvelope(
    int SchemaVersion,
    string EventId,
    string Queue,
    string RunId)
{
    public const int CurrentSchemaVersion = 1;

    public static ExecutionEnvelope FromWorkAvailableSignal(
        WorkAvailableSignal signal)
    {
        ArgumentNullException.ThrowIfNull(signal);
        return new ExecutionEnvelope(
            CurrentSchemaVersion,
            signal.EventId,
            signal.Queue,
            signal.RunId);
    }
}

/// <summary>
/// Publishes execution envelopes to a broker-backed execution adapter. The
/// adapter must not treat publication as a lease or bypass completion fencing.
/// </summary>
public interface IExecutionDispatcher
{
    ValueTask DispatchAsync(
        ExecutionEnvelope envelope,
        CancellationToken cancellationToken);
}
