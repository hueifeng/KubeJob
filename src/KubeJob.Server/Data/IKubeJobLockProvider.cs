using System;
using System.Threading;
using System.Threading.Tasks;

namespace KubeJob.Server.Data
{
    /// <summary>
    /// Provides distributed locking mechanisms for leader election and mutual exclusion.
    /// </summary>
    public interface IKubeJobLockProvider
    {
        /// <summary>
        /// Attempts to acquire a distributed lock.
        /// </summary>
        /// <param name="lockName">The unique name of the lock.</param>
        /// <param name="holderId">The identifier of the node attempting to acquire the lock.</param>
        /// <param name="expiry">The duration for which the lock should be held.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>True if the lock was successfully acquired; otherwise, false.</returns>
        Task<bool> TryAcquireLockAsync(string lockName, string holderId, TimeSpan expiry, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Attempts to renew an already held lock.
        /// </summary>
        /// <param name="lockName">The unique name of the lock.</param>
        /// <param name="holderId">The identifier of the node currently holding the lock.</param>
        /// <param name="expiry">The duration to extend the lock.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>True if the lock was successfully renewed; otherwise, false.</returns>
        Task<bool> RenewLockAsync(string lockName, string holderId, TimeSpan expiry, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Releases a held lock.
        /// </summary>
        /// <param name="lockName">The unique name of the lock.</param>
        /// <param name="holderId">The identifier of the node holding the lock.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task ReleaseLockAsync(string lockName, string holderId, CancellationToken cancellationToken = default);
    }
}
