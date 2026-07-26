using KubeJob.Core.Client;
using KubeJob.Server.ControlPlane;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/jobs/{runId}/attempts")]
public sealed class JobAttemptSnapshotsController : ControllerBase
{
    private readonly JobControlPlane _controlPlane;

    public JobAttemptSnapshotsController(JobControlPlane controlPlane)
    {
        _controlPlane = controlPlane;
    }

    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<JobAttemptSnapshot>>> Get(
        string runId,
        CancellationToken cancellationToken)
    {
        var attempts = await _controlPlane.GetAttemptsAsync(runId, cancellationToken);
        if (attempts is null)
        {
            return NotFound();
        }

        return Ok(attempts);
    }
}
