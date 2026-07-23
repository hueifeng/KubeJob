using System;
using System.Collections.Generic;

namespace KubeJob.Worker.Options
{
    /// <summary>
    /// Configuration options for a KubeJob Worker Node.
    /// </summary>
    public class KubeJobWorkerOptions
    {
        /// <summary>
        /// HTTP endpoint of the KubeJob control plane.
        /// </summary>
        public string ServerEndpoint { get; set; } = "http://localhost:5000";

        /// <summary>
        /// Maximum number of handlers executing concurrently in this process.
        /// </summary>
        public int MaxConcurrentJobs { get; set; } = 10;

        /// <summary>
        /// Stable worker identity. Each process start still receives a unique session identity and epoch.
        /// </summary>
        public string WorkerId { get; set; } = Environment.MachineName;

        public Dictionary<string, string> Labels { get; set; } = new();

        /// <summary>
        /// Queues this worker is allowed to claim from.
        /// </summary>
        public List<string> Queues { get; set; } = new() { "default" };

        public string BuildId { get; set; } = "unknown";

        /// <summary>
        /// Enables the V2 pull/attempt/lease runtime. Legacy WorkerAgentService remains available for migration.
        /// </summary>
        public bool EnableRuntimeV2 { get; set; }

        public int ClaimBatchSize { get; set; } = 16;

        public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromSeconds(1);

        public TimeSpan LeaseRenewalInterval { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(5);

        public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

        public void ValidateV2()
        {
            if (string.IsNullOrWhiteSpace(ServerEndpoint))
            {
                throw new InvalidOperationException("ServerEndpoint is required.");
            }

            if (string.IsNullOrWhiteSpace(WorkerId))
            {
                throw new InvalidOperationException("WorkerId is required.");
            }

            if (MaxConcurrentJobs < 1)
            {
                throw new InvalidOperationException("MaxConcurrentJobs must be positive.");
            }

            if (ClaimBatchSize is < 1 or > 1024)
            {
                throw new InvalidOperationException("ClaimBatchSize must be between 1 and 1024.");
            }

            if (Queues.Count == 0)
            {
                throw new InvalidOperationException("At least one queue is required.");
            }
        }
    }
}
