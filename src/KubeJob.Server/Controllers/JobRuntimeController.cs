using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/runtime")]
public sealed class JobRuntimeController : ControllerBase
{
    private readonly IWorkerSessionStore _workerSessions;
    private readonly IJobClaimStore _claims;
    private readonly IJobCompletionStore _completions;
    private readonly JobRuntimeOptions _options;

    public JobRuntimeController(
        IWorkerSessionStore workerSessions,
        IJobClaimStore claims,
        IJobCompletionStore completions,
        IOptions<JobRuntimeOptions> options)
    {
        _workerSessions = workerSessions;
        _claims = claims;
        _completions = completions;
        _options = options.Value;
    }

    [HttpPost("workers/register")]
    public async Task<ActionResult<RegisterWorkerSessionResponse>> RegisterWorker(
        [FromBody] RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.WorkerId)
            || string.IsNullOrWhiteSpace(request.SessionId)
            || request.MaxConcurrency < 1
            || request.Queues.Count == 0
            || request.Capabilities.Count == 0)
        {
            return BadRequest("Worker identity, positive capacity, queues, and capabilities are required.");
        }

        var session = await _workerSessions.RegisterAsync(request, cancellationToken);
        return Ok(new RegisterWorkerSessionResponse(
            session.WorkerId,
            session.SessionId,
            session.Epoch,
            session.StartedAt));
    }

    [HttpPost("workers/heartbeat")]
    public async Task<IActionResult> Heartbeat(
        [FromBody] WorkerHeartbeatRequest request,
        CancellationToken cancellationToken)
    {
        var accepted = await _workerSessions.HeartbeatAsync(request, cancellationToken);
        return accepted ? Ok() : Conflict(new { reason = "stale_worker_session" });
    }

    [HttpPost("workers/close")]
    public async Task<IActionResult> Close(
        [FromBody] WorkerHeartbeatRequest request,
        CancellationToken cancellationToken)
    {
        var accepted = await _workerSessions.CloseAsync(
            request.WorkerId,
            request.SessionId,
            request.SessionEpoch,
            cancellationToken);
        return accepted ? Ok() : Conflict(new { reason = "stale_worker_session" });
    }

    [HttpPost("claims")]
    public async Task<ActionResult<ClaimJobsResponse>> Claim(
        [FromBody] ClaimJobsRequest request,
        CancellationToken cancellationToken)
    {
        if (request.AvailableSlots <= 0)
        {
            return Ok(new ClaimJobsResponse(Array.Empty<ClaimedJob>()));
        }

        var jobs = await _claims.ClaimAsync(
            request,
            _options.LeaseDuration,
            _options.MaxClaimBatchSize,
            cancellationToken);
        return Ok(new ClaimJobsResponse(jobs));
    }

    [HttpPost("leases/renew")]
    public async Task<ActionResult<RenewLeasesResponse>> Renew(
        [FromBody] RenewLeasesRequest request,
        CancellationToken cancellationToken)
    {
        var attempts = await _claims.RenewLeasesAsync(
            request,
            _options.LeaseDuration,
            cancellationToken);
        return Ok(new RenewLeasesResponse(attempts));
    }

    [HttpPost("attempts/complete")]
    public async Task<ActionResult<CompleteAttemptResponse>> Complete(
        [FromBody] CompleteAttemptRequest request,
        CancellationToken cancellationToken)
    {
        var result = await _completions.CompleteAsync(
            request,
            _options.RetryDelay,
            cancellationToken);

        return result.Accepted ? Ok(result) : Conflict(result);
    }
}
