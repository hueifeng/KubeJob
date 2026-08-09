using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/jobs")]
public sealed class JobsApiController : ControllerBase
{
    private readonly JobControlPlane _controlPlane;
    private readonly DefaultJobClient _client;

    public JobsApiController(JobControlPlane controlPlane, DefaultJobClient client)
    {
        _controlPlane = controlPlane;
        _client = client;
    }

    [HttpPost]
    public async Task<ActionResult<JobHandle>> Enqueue(
        [FromBody] EnqueueJobRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            var receipt = await _client.SubmitAsync(request, cancellationToken);
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
        catch (NotSupportedException unsupported)
        {
            return BadRequest(new
            {
                code = "unsupported_job_submission",
                message = unsupported.Message
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

    [HttpPost("batch")]
    public async Task<ActionResult<IReadOnlyList<JobHandle>>> EnqueueBatch(
        [FromBody] IReadOnlyList<EnqueueJobRequest> requests,
        CancellationToken cancellationToken)
    {
        try
        {
            var receipts = await _client.SubmitBatchAsync(requests, cancellationToken);
            var handles = receipts.Select(receipt => receipt.Handle).ToArray();
            return receipts.Any(receipt => !receipt.Existing)
                ? Accepted(handles)
                : Ok(handles);
        }
        catch (ControlPlaneValidationException validation)
        {
            return BadRequest(new
            {
                code = validation.Code,
                message = validation.Message
            });
        }
        catch (NotSupportedException unsupported)
        {
            return BadRequest(new
            {
                code = "unsupported_job_submission",
                message = unsupported.Message
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
