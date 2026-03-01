using System;
using KubeJob.Core.Enums;

namespace KubeJob.Core.Domain
{
    public class JobRun
    {
        public string Id { get; set; } = string.Empty;
        public string SpecId { get; set; } = string.Empty;
        public string JobType { get; set; } = string.Empty;
        public int TimeoutSeconds { get; set; } = 300;

        public string BatchId { get; set; } = string.Empty;
        public int ShardIndex { get; set; }
        public JobStatus Status { get; set; }
        public string TargetNodeId { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }
        public string ResultMsg { get; set; } = string.Empty;
        public string? RowVersion { get; set; } = Guid.NewGuid().ToString();
    }
}
