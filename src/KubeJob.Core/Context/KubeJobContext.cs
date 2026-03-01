using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace KubeJob.Core.Context
{
    /// <summary>
    /// Provides context information for a currently executing job.
    /// </summary>
    public class KubeJobContext
    {
        /// <summary>
        /// Unique identifier for this specific execution run.
        /// </summary>
        public string RunId { get; set; } = string.Empty;

        /// <summary>
        /// The identifier of the Job Specification that spawned this run.
        /// </summary>
        public string SpecId { get; set; } = string.Empty;

        /// <summary>
        /// For broadcast or sharding jobs, this groups multiple runs together.
        /// </summary>
        public string BatchId { get; set; } = string.Empty;

        /// <summary>
        /// The zero-based index of the shard assigned to this node. 
        /// </summary>
        public int ShardIndex { get; set; }

        /// <summary>
        /// The total number of shards the job was split into.
        /// </summary>
        public int TotalShards { get; set; }
        
        /// <summary>
        /// A scoped service provider for resolving dependencies within the job.
        /// </summary>
        public IServiceProvider ServiceProvider { get; set; } = default!;

        /// <summary>
        /// A logger specifically scoped to this job run.
        /// </summary>
        public ILogger Logger { get; set; } = default!;
    }
}
