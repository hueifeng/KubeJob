using System;
using System.Collections.Generic;

namespace KubeJob.Worker.Options
{
    /// <summary>
    /// Configuration options for the KubeJob Worker Node.
    /// </summary>
    public class KubeJobWorkerOptions
    {
        /// <summary>
        /// The HTTP endpoint of the KubeJob Server (Control Plane).
        /// Default is http://localhost:5000.
        /// </summary>
        public string ServerEndpoint { get; set; } = "http://localhost:5000";

        /// <summary>
        /// Maximum number of jobs this worker can execute concurrently.
        /// Default is 10.
        /// </summary>
        public int MaxConcurrentJobs { get; set; } = 10;

        /// <summary>
        /// Unique identifier for this worker. Defaults to the Machine Name.
        /// </summary>
        public string WorkerId { get; set; } = Environment.MachineName;

        /// <summary>
        /// Custom labels assigned to this worker, used for NodeSelector matching.
        /// </summary>
        public Dictionary<string, string> Labels { get; set; } = new();
    }
}
