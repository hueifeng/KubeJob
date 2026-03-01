using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using KubeJob.Core.Domain;

namespace KubeJob.Server.Data
{
    public class InMemoryLockProvider : IKubeJobLockProvider
    {
        private readonly ConcurrentDictionary<string, DistributedLease> _leases = new();

        public Task<bool> TryAcquireLockAsync(string lockName, string holderId, TimeSpan expiry, CancellationToken cancellationToken = default)
        {
            var now = DateTime.UtcNow;
            var acquired = false;

            _leases.AddOrUpdate(lockName,
                // Add
                key => 
                {
                    acquired = true;
                    return new DistributedLease
                    {
                        LockName = key,
                        HolderId = holderId,
                        AcquiredAt = now,
                        ExpiresAt = now.Add(expiry)
                    };
                },
                // Update
                (key, existing) =>
                {
                    // If we already hold it, or it expired, we can acquire it
                    if (existing.HolderId == holderId || existing.ExpiresAt < now)
                    {
                        acquired = true;
                        existing.HolderId = holderId;
                        existing.AcquiredAt = now;
                        existing.ExpiresAt = now.Add(expiry);
                    }
                    return existing;
                });

            return Task.FromResult(acquired);
        }

        public Task<bool> RenewLockAsync(string lockName, string holderId, TimeSpan expiry, CancellationToken cancellationToken = default)
        {
            var now = DateTime.UtcNow;

            if (_leases.TryGetValue(lockName, out var existing))
            {
                if (existing.HolderId == holderId && existing.ExpiresAt >= now)
                {
                    existing.ExpiresAt = now.Add(expiry);
                    return Task.FromResult(true);
                }
            }

            return Task.FromResult(false);
        }

        public Task ReleaseLockAsync(string lockName, string holderId, CancellationToken cancellationToken = default)
        {
            if (_leases.TryGetValue(lockName, out var existing))
            {
                if (existing.HolderId == holderId)
                {
                    _leases.TryRemove(lockName, out _);
                }
            }
            return Task.CompletedTask;
        }
    }
}
