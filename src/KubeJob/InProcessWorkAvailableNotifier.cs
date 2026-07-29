using KubeJob.Core.Runtime;
using KubeJob.Worker.Runtime;

namespace KubeJob;

/// <summary>
/// Bridges the transactional Outbox's work-available signal directly to the
/// in-process Worker claim loop for unified hosts. Unified deployments run
/// the control plane and Worker in the same process, so the Outbox publisher
/// and <see cref="IWorkerClaimTriggerSource"/> already share a container;
/// this notifier pulses that trigger instead of leaving Pull-mode Workers to
/// discover new work only on their next polling interval.
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
