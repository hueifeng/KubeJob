using System.Text.Json.Serialization;
using KubeJob.Core.Domain;
using KubeJob.Core.Dtos;

namespace KubeJob.Worker.Runtime;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    GenerationMode = JsonSourceGenerationMode.Default)]
[JsonSerializable(typeof(RegisterWorkerSessionRequest))]
[JsonSerializable(typeof(RegisterWorkerSessionResponse))]
[JsonSerializable(typeof(WorkerCapabilityDto))]
[JsonSerializable(typeof(JobDefinitionDto))]
[JsonSerializable(typeof(ClaimRunsRequest))]
[JsonSerializable(typeof(ClaimRunsResponse))]
[JsonSerializable(typeof(JobLease))]
[JsonSerializable(typeof(RenewLeasesRequest))]
[JsonSerializable(typeof(RenewLeasesResponse))]
[JsonSerializable(typeof(CompleteRunRequest))]
internal partial class KubeJobRuntimeJsonContext : JsonSerializerContext { }
