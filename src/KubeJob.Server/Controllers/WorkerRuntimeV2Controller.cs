using System.Text.Json;
using System.Diagnostics;
using KubeJob.Core.Domain;
using KubeJob.Core.Dtos;
using KubeJob.Core.Enums;
using KubeJob.Server.Data;
using Microsoft.AspNetCore.Mvc;
using KubeJob.Server.Telemetry;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/runtime")]
public sealed class WorkerRuntimeV2Controller : ControllerBase
{
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(30);
    private readonly IKubeJobRuntimeRepository _repository;
    private readonly IJobAvailabilitySignal _availability;

    public WorkerRuntimeV2Controller(
        IKubeJobRuntimeRepository repository,
        IJobAvailabilitySignal availability)
    {
        _repository = repository;
        _availability = availability;
    }

    [HttpPost("register")]
    public async Task<ActionResult<RegisterWorkerSessionResponse>> Register(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        if (!ValidIdentity(request.WorkerId, request.SessionId) || request.MaxCapacity is <= 0 or > 4096)
            return BadRequest("Invalid worker identity or capacity.");
        if (request.Capabilities.Count is 0 or > 4096 || request.Definitions.Count != request.Capabilities.Count)
            return BadRequest("Capabilities and definitions must be non-empty and have equal length.");
        if (request.Labels.Count > 128 || request.Labels.Any(static x =>
                string.IsNullOrWhiteSpace(x.Key) || x.Key.Length > 100 || (x.Value?.Length ?? 0) > 200))
            return BadRequest("Worker labels exceed limits.");
        if (request.Capabilities.Any(static x => string.IsNullOrWhiteSpace(x.JobType) || x.JobType.Length > 200 ||
                x.PayloadSchemaVersion <= 0 || x.HandlerVersion.Length > 64))
            return BadRequest("Invalid capability.");
        if (request.Definitions.Any(static x => string.IsNullOrWhiteSpace(x.Name) || x.Name.Length > 200 ||
                x.Cron.Length > 100 || x.TotalShards is <= 0 or > 4096 || x.TimeoutSeconds is <= 0 or > 604800 ||
                x.MaxRetries is < 0 or > 1000 || x.NodeSelectors.Count > 128))
            return BadRequest("Invalid job definition.");

        var capabilityNames = request.Capabilities.Select(static x => x.JobType).ToHashSet(StringComparer.Ordinal);
        var definitionNames = request.Definitions.Select(static x => x.Name).ToHashSet(StringComparer.Ordinal);
        if (capabilityNames.Count != request.Capabilities.Count ||
            definitionNames.Count != request.Definitions.Count || !capabilityNames.SetEquals(definitionNames))
            return BadRequest("Capability and definition names must be unique and match.");

        var labelsJson = JsonSerializer.Serialize(request.Labels);
        if (labelsJson.Length > 32_768) return BadRequest("Serialized labels are too large.");

        var epoch = await _repository.RegisterWorkerSessionAsync(request, labelsJson, cancellationToken);
        return Ok(new RegisterWorkerSessionResponse
        {
            SessionEpoch = epoch,
            HeartbeatInterval = HeartbeatInterval,
            LeaseDuration = LeaseDuration
        });
    }

    [HttpPost("claim")]
    public async Task<ActionResult<ClaimRunsResponse>> Claim(ClaimRunsRequest request, CancellationToken cancellationToken)
    {
        using var activity = KubeJobTelemetry.ActivitySource.StartActivity("kubejob.claim");
        var started = Stopwatch.GetTimestamp();
        if (!ValidSession(request.WorkerId, request.SessionId, request.SessionEpoch)) return BadRequest();
        if (request.QueueNames.Count > 64 || request.WaitMilliseconds is < 0 or > 25_000 ||
            request.QueueNames.Any(static x => string.IsNullOrWhiteSpace(x) || x.Length > 100))
            return BadRequest("Invalid queue filter or wait duration.");
        if (request.AvailableSlots <= 0) return Ok(new ClaimRunsResponse());

        var version = _availability.Version;
        var leases = await ClaimOnceAsync(request, cancellationToken);
        if (leases.Count == 0 && request.WaitMilliseconds > 0)
        {
            await _availability.WaitForChangeAsync(version, TimeSpan.FromMilliseconds(request.WaitMilliseconds), cancellationToken);
            leases = await ClaimOnceAsync(request, cancellationToken);
        }
        KubeJobTelemetry.Claims.Add(leases.Count);
        KubeJobTelemetry.ClaimLatency.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        return Ok(new ClaimRunsResponse { Leases = leases });
    }

    [HttpPost("renew")]
    public async Task<ActionResult<RenewLeasesResponse>> Renew(RenewLeasesRequest request, CancellationToken cancellationToken)
    {
        if (!ValidSession(request.WorkerId, request.SessionId, request.SessionEpoch) ||
            request.Leases.Count > 4096 || request.CurrentLoad is < 0 or > 4096 ||
            request.Leases.Any(static x => string.IsNullOrWhiteSpace(x.RunId) || x.RunId.Length > 100 || x.LeaseToken <= 0))
            return BadRequest();
        return Ok(await _repository.RenewLeasesAsync(request, LeaseDuration, cancellationToken));
    }

    [HttpPost("complete")]
    public async Task<IActionResult> Complete(CompleteRunRequest request, CancellationToken cancellationToken)
    {
        if (!ValidSession(request.WorkerId, request.SessionId, request.SessionEpoch) ||
            string.IsNullOrWhiteSpace(request.RunId) || request.RunId.Length > 100 || request.LeaseToken <= 0 ||
            request.Status is not (JobStatus.Succeeded or JobStatus.Failed or JobStatus.Canceled) ||
            request.ResultSummary.Length > 16_384)
            return BadRequest();
        var accepted = await _repository.TryCompleteRunAsync(request, cancellationToken);
        KubeJobTelemetry.Completions.Add(1);
        if (!accepted) KubeJobTelemetry.FencedRejects.Add(1);
        return accepted ? Ok() : Conflict();
    }

    private Task<IReadOnlyList<JobLease>> ClaimOnceAsync(ClaimRunsRequest request, CancellationToken cancellationToken)
    {
        return _repository.ClaimRunsAsync(request.WorkerId, request.SessionId, request.SessionEpoch,
            request.QueueNames, Math.Clamp(request.AvailableSlots, 1, 256), LeaseDuration, cancellationToken);
    }

    private static bool ValidSession(string workerId, string sessionId, long epoch) =>
        ValidIdentity(workerId, sessionId) && epoch > 0;
    private static bool ValidIdentity(string workerId, string sessionId) =>
        !string.IsNullOrWhiteSpace(workerId) && workerId.Length <= 100 &&
        !string.IsNullOrWhiteSpace(sessionId) && sessionId.Length <= 64;
}
