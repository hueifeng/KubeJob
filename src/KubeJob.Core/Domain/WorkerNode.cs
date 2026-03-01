using System;

namespace KubeJob.Core.Domain
{
    public class WorkerNode
    {
        public string Id { get; set; } = string.Empty;
        public string IpAddress { get; set; } = string.Empty;
        
        /// <summary>
        /// JSON string representation of the node labels.
        /// </summary>
        public string Labels { get; set; } = "{}";
        
        public DateTime LastHeartbeat { get; set; }
        public int CurrentLoad { get; set; }
        public int MaxCapacity { get; set; }
        public bool IsOffline { get; set; }
    }
}
