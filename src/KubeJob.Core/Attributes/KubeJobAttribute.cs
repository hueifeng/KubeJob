using System;
using KubeJob.Core.Enums;

namespace KubeJob.Core.Attributes
{
    /// <summary>
    /// Decorates a class to be registered as a KubeJob.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class KubeJobAttribute : Attribute
    {
        /// <summary>
        /// The unique job type name.
        /// </summary>
        public string Name { get; }
        
        /// <summary>
        /// A cron expression for scheduling. If empty, the job must be triggered manually.
        /// </summary>
        public string Cron { get; set; } = string.Empty;
        
        /// <summary>
        /// Determines if the job is standalone, sharded, or broadcast.
        /// </summary>
        public ExecuteModel ExecuteModel { get; set; } = ExecuteModel.Standalone;
        
        /// <summary>
        /// Number of shards for Sharding execute model.
        /// </summary>
        public int TotalShards { get; set; } = 1;
        
        /// <summary>
        /// Max execution time allowed before the job is canceled (ActiveDeadlineSeconds).
        /// </summary>
        public int TimeoutSeconds { get; set; } = 300;
        
        /// <summary>
        /// Number of retries allowed on failure (BackoffLimit).
        /// </summary>
        public int MaxRetries { get; set; } = 0;

        public KubeJobAttribute(string name)
        {
            Name = name;
        }
    }

    /// <summary>
    /// Specifies that a job should only be assigned to worker nodes containing a matching label.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public class NodeSelectorAttribute : Attribute
    {
        public string Key { get; }
        public string Value { get; }

        public NodeSelectorAttribute(string key, string value)
        {
            Key = key;
            Value = value;
        }
    }
}
