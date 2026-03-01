using System.Threading;
using System.Threading.Tasks;
using KubeJob.Core.Context;

namespace KubeJob.Core.Interfaces
{
    /// <summary>
    /// The fundamental interface for defining a background job in KubeJob.
    /// Classes implementing this interface must also be decorated with [KubeJob].
    /// </summary>
    public interface IKubeJob
    {
        /// <summary>
        /// Executes the job logic.
        /// </summary>
        /// <param name="context">Provides information about the current execution (RunId, ShardIndex, Logger, ServiceProvider).</param>
        /// <param name="token">A cancellation token that is triggered upon system shutdown or timeout.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task ExecuteAsync(KubeJobContext context, CancellationToken token);
    }
}
