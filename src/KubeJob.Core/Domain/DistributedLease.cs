using System;

namespace KubeJob.Core.Domain
{
    public class DistributedLease
    {
        public string LockName { get; set; } = string.Empty;
        public string HolderId { get; set; } = string.Empty;
        public DateTime AcquiredAt { get; set; }
        public DateTime ExpiresAt { get; set; }
    }
}
