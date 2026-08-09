using KubeJob.Core.Runtime;

namespace KubeJob.ControlPlane.Runtime;

/// <summary>
/// Finalizes a completion that has already crossed the durable completion-intent
/// boundary. Implementations must validate that the persisted intent still owns
/// the Run's current Attempt/Fence, but must not require the original worker
/// lease or session to still be alive.
/// </summary>
public interface ICompletionIntentFinalizer
{
    ValueTask<CompleteAttemptResponse> FinalizeAsync(
        CompleteAttemptRequest request,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<CompleteAttemptResponse>> FinalizeBatchAsync(
        IReadOnlyList<CompleteAttemptRequest> requests,
        RetryPolicy retryPolicy,
        CancellationToken cancellationToken);
}
