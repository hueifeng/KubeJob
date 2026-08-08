using KubeJob.Core.Runtime;
using KubeJob.Worker.Runtime;

namespace KubeJob;

/// <summary>
/// Bridges PostgresManaged work-available hints directly to the in-process
/// Worker claim loop for unified hosts. Immediate submissions reach this
/// notifier through <see cref="ControlPlane.Runtime.ManagedWorkAvailableDispatcher"/>;
/// delayed/recovery rows may still reach it through the durable outbox
/// publisher. In both cases the signal only pulses claim discovery and never
/// grants execution ownership.
/// </summary>
public sealed class InProcessWorkAvailableNotifier : IWorkAvailableNotifier
{
    private readonly IWorkerClaimTriggerSource _claimTrigger;

    public InProcessWorkAvailableNotifier(IWorkerClaimTriggerSource claimTrigger)
    {
        _claimTrigger = claimTrigger;
    }

    public ValueTask PublishAsync(
        WorkAvailableSignal signal,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _claimTrigger.Pulse();
        return ValueTask.CompletedTask;
    }
}
