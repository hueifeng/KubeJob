using System;
using System.Collections.Generic;
using KubeJob.Core.Domain;
using KubeJob.Core.Enums;

namespace KubeJob.Core.Dtos
{
    public class HeartbeatRequest
    {
        public string WorkerId { get; set; } = string.Empty;
        public string IpAddress { get; set; } = string.Empty;
        public Dictionary<string, string> Labels { get; set; } = new();
        public int CurrentLoad { get; set; }
        public int MaxCapacity { get; set; }
        public List<string> RunningJobIds { get; set; } = new();
    }

    public class PollJobsResponse
    {
        public List<JobRun> Jobs { get; set; } = new();
    }

    public class JobReportRequest
    {
        public string WorkerId { get; set; } = string.Empty;
        public string RunId { get; set; } = string.Empty;
        public JobStatus Status { get; set; }
        public string ResultMsg { get; set; } = string.Empty;
    }

    public class RegisterJobsRequest
    {
        public string WorkerId { get; set; } = string.Empty;
        public List<JobRegistrationDto> Jobs { get; set; } = new();
    }

    public class JobRegistrationDto
    {
        public string Name { get; set; } = string.Empty;
        public string Cron { get; set; } = string.Empty;
        public ExecuteModel ExecuteModel { get; set; } = ExecuteModel.Standalone;
        public int TotalShards { get; set; } = 1;
        public int TimeoutSeconds { get; set; } = 300;
        public int MaxRetries { get; set; } = 0;
        public Dictionary<string, string> NodeSelectors { get; set; } = new();
    }
}
