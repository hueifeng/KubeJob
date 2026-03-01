using System;

namespace KubeJob.Core.Enums
{
    /// <summary>
    /// Defines how a job should be executed across worker nodes.
    /// </summary>
    public enum ExecuteModel
    {
        /// <summary>
        /// Executes on a single randomly selected available node.
        /// </summary>
        Standalone = 0,

        /// <summary>
        /// Splits the job into multiple shards and executes across multiple nodes.
        /// </summary>
        Sharding = 1,

        /// <summary>
        /// Executes on every single available worker node simultaneously.
        /// </summary>
        Broadcast = 2
    }
}