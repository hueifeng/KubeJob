using System;
using KubeJob.Core.Enums;

namespace KubeJob.Core.Domain
{
    public class JobSpec
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string JobType { get; set; } = string.Empty;
        public string CronExpression { get; set; } = string.Empty;
        
        /// <summary>
        /// JSON string representation of the node selector labels.
        /// </summary>
        public string NodeSelector { get; set; } = "{}";
        
        public ExecuteModel ExecuteModel { get; set; }
        public int TotalShards { get; set; }
        public DateTime? NextRunTime { get; set; }
        public bool IsDisabled { get; set; }
        
        /// <summary>
        /// activeDeadlineSeconds
        /// </summary>
        public int TimeoutSeconds { get; set; }
        
        /// <summary>
        /// backoffLimit
        /// </summary>
        public int MaxRetries { get; set; }
        
        /// <summary>
        /// concurrencyPolicy
        /// </summary>
        public ConcurrencyPolicy ConcurrencyPolicy { get; set; }
        
        /// <summary>
        /// successfulJobsHistoryLimit
        /// </summary>
        public int SuccessfulJobsHistoryLimit { get; set; } = 3;
        
        /// <summary>
        /// failedJobsHistoryLimit
        /// </summary>
        public int FailedJobsHistoryLimit { get; set; } = 1;
    }
}
