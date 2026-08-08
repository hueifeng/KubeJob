using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/runtime")]
public sealed class JobRuntimeController : ControllerBase
{
    private readonly WorkerControlPlane _controlPlane;

    public JobRuntimeController(WorkerControlPlane controlPlane)
    {
        _controlPlane = controlPlane;
    }

    [HttpPost("workers/register")]
    public async Task<ActionResult<RegisterWorkerSessionResponse>> RegisterWorker(
        [FromBody] RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            return Ok(await _controlPlane.RegisterAsync(request, cancellationToken));
        }
        catch (ControlPlaneValidationException validation)
        {
            return BadRequest(new
            {
                code = validation.Code,
                message = validation.Message
            });
        }
    }

    [HttpPost("workers/heartbeat")]
    public async Task<IActionResult> Heartbeat(
        [FromBody] WorkerHeartbeatRequest request,
        CancellationToken cancellationToken)
    {
        var accepted = await _controlPlane.HeartbeatAsync(request, cancellationToken);
        return accepted ? Ok() : Conflict(new { reason = "stale_worker_session" });
    }

    [HttpPost("workers/close")]
    public async Task<IActionResult> Close(
        [FromBody] WorkerHeartbeatRequest request,
        CancellationToken cancellationToken)
    {
        var accepted = await _controlPlane.CloseAsync(request, cancellationToken);
        return accepted ? Ok() : Conflict(new { reason = "stale_worker_session" });
    }

    [HttpPost("claims")]
    public async Task<ActionResult<ClaimJobsResponse>> Claim(
        [FromBody] ClaimJobsRequest request,
        CancellationToken cancellationToken)
    {
        return Ok(await _controlPlane.ClaimAsync(request, cancellationToken));
    }

    [HttpPost("leases/renew")]
    public async Task<ActionResult<RenewLeasesResponse>> Renew(
        [FromBody] RenewLeasesRequest request,
        CancellationToken cancellationToken)
    {
        return Ok(await _controlPlane.RenewLeasesAsync(request, cancellationToken));
    }

    [HttpPost("executions/requeue")]
    public async Task<IActionResult> RequeueExecution(
        [FromBody] RequeueExecutionRequest request,
        CancellationToken cancellationToken)
    {
        var accepted = await _controlPlane.RequeueExecutionAsync(request, cancellationToken);
        return accepted ? Ok() : Conflict(new { reason = "execution_no_longer_pending" });
    }

    [HttpPost("attempts/complete")]
    public async Task<ActionResult<CompleteAttemptResponse>> Complete(
        [FromBody] CompleteAttemptRequest request,
        CancellationToken cancellationToken)
    {
        var result = await _controlPlane.CompleteAsync(request, cancellationToken);
        return result.Accepted ? Ok(result) : Conflict(result);
    }
}
