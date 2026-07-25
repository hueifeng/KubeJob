using KubeJob.Core.Execution;

namespace KubeJob.Core.Interfaces;

/// <summary>
/// Defines a strongly typed background job handler.
/// </summary>
/// <typeparam name="TPayload">The payload contract accepted by the handler.</typeparam>
public interface IKubeJob<in TPayload>
{
    /// <summary>
    /// Executes one job attempt.
    /// </summary>
    ValueTask ExecuteAsync(
        TPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken);
}
