using KubeJob.Core.Domain;
using KubeJob.Core.Enums;

namespace KubeJob.Core.Dtos;

public sealed class WorkerCapabilityDto
{
    public string JobType { get; set; } = string.Empty;
    public string HandlerVersion { get; set; } = string.Empty;
    public int PayloadSchemaVersion { get; set; } = 1;
}

public sealed class JobDefinitionDto
{
    public string Name { get; set; } = string.Empty;
    public string Cron { get; set; } = string.Empty;
    public ExecuteModel ExecuteModel { get; set; } = ExecuteModel.Standalone;
    public int TotalShards { get; set; } = 1;
    public int TimeoutSeconds { get; set; } = 300;
    public int MaxRetries { get; set; }
    public IReadOnlyDictionary<string, string> NodeSelectors { get; set; } = new Dictionary<string, string>();
}

public sealed class RegisterWorkerSessionRequest
{
    public string WorkerId { get; set; } = string.Empty;
    public string SessionId { get; set; } = string.Empty;
    public string RuntimeVersion { get; set; } = string.Empty;
    public string IpAddress { get; set; } = string.Empty;
    public IReadOnlyDictionary<string, string> Labels { get; set; } = new Dictionary<string, string>();
    public IReadOnlyList<WorkerCapabilityDto> Capabilities { get; set; } = Array.Empty<WorkerCapabilityDto>();
    public IReadOnlyList<JobDefinitionDto> Definitions { get; set; } = Array.Empty<JobDefinitionDto>();
    public int MaxCapacity { get; set; }
}

public sealed class RegisterWorkerSessionResponse
{
    public long SessionEpoch { get; init; }
    public TimeSpan HeartbeatInterval { get; init; }
    public TimeSpan LeaseDuration { get; init; }
}

public sealed class ClaimRunsRequest
{
    public string WorkerId { get; set; } = string.Empty;
    public string SessionId { get; set; } = string.Empty;
    public long SessionEpoch { get; set; }
    public IReadOnlyList<string> QueueNames { get; set; } = Array.Empty<string>();
    public int AvailableSlots { get; set; }
    public int WaitMilliseconds { get; set; }
}

public sealed class ClaimRunsResponse
{
    public IReadOnlyList<JobLease> Leases { get; init; } = Array.Empty<JobLease>();
}

public sealed class LeaseRenewalDto
{
    public string RunId { get; set; } = string.Empty;
    public long LeaseToken { get; set; }
}

public sealed class RenewLeasesRequest
{
    public string WorkerId { get; set; } = string.Empty;
    public string SessionId { get; set; } = string.Empty;
    public long SessionEpoch { get; set; }
    public IReadOnlyList<LeaseRenewalDto> Leases { get; set; } = Array.Empty<LeaseRenewalDto>();
    public int CurrentLoad { get; set; }
    public bool Draining { get; set; }
}

public sealed class RenewLeasesResponse
{
    public IReadOnlyList<string> RejectedRunIds { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> CancelRunIds { get; init; } = Array.Empty<string>();
}

public sealed class CompleteRunRequest
{
    public string WorkerId { get; set; } = string.Empty;
    public string SessionId { get; set; } = string.Empty;
    public long SessionEpoch { get; set; }
    public string RunId { get; set; } = string.Empty;
    public long LeaseToken { get; set; }
    public JobStatus Status { get; set; }
    public string ResultSummary { get; set; } = string.Empty;
}
