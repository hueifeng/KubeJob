using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/jobs")]
public sealed class JobsApiController : ControllerBase
{
    private readonly JobControlPlane _controlPlane;

    public JobsApiController(JobControlPlane controlPlane)
    {
        _controlPlane = controlPlane;
    }

    [HttpPost]
    public async Task<ActionResult<JobHandle>> Enqueue(
        [FromBody] EnqueueJobRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            var receipt = await _controlPlane.SubmitAsync(request, cancellationToken);
            return receipt.Existing ? Ok(receipt.Handle) : Accepted(receipt.Handle);
        }
        catch (ControlPlaneValidationException validation)
        {
            return BadRequest(new
            {
                code = validation.Code,
                message = validation.Message
            });
        }
        catch (IdempotencyConflictException conflict)
        {
            return Conflict(new
            {
                code = "idempotency_conflict",
                conflict.IdempotencyKey,
                conflict.ExistingJobId
            });
        }
    }

    [HttpGet("{runId}")]
    public async Task<ActionResult<JobStatusSnapshot>> GetStatus(
        string runId,
        CancellationToken cancellationToken)
    {
        var snapshot = await _controlPlane.GetStatusAsync(runId, cancellationToken);
        if (snapshot is null)
        {
            return NotFound();
        }

        return Ok(snapshot);
    }

    [HttpPost("{runId}/cancel")]
    public async Task<IActionResult> Cancel(
        string runId,
        [FromBody] CancelJobRequest? request,
        CancellationToken cancellationToken)
    {
        var accepted = await _controlPlane.RequestCancelAsync(
            runId,
            request?.Reason,
            cancellationToken);
        return accepted ? Accepted() : NotFound();
    }
}
