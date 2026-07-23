using KubeJob.Core.Client;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/jobs/{runId}/attempts")]
public sealed class JobAttemptSnapshotsController : ControllerBase
{
    private readonly IJobQueryStore _queries;

    public JobAttemptSnapshotsController(IJobQueryStore queries)
    {
        _queries = queries;
    }

    [HttpGet(Order = -100)]
    public async Task<ActionResult<IReadOnlyList<JobAttemptSnapshot>>> Get(
        string runId,
        CancellationToken cancellationToken)
    {
        if (await _queries.GetRunAsync(runId, cancellationToken) is null)
        {
            return NotFound();
        }

        var attempts = await _queries.GetAttemptsAsync(runId, cancellationToken);
        return Ok(attempts.Select(attempt => new JobAttemptSnapshot(
            attempt.Id,
            attempt.AttemptNumber,
            attempt.WorkerId,
            attempt.SessionId,
            attempt.SessionEpoch,
            attempt.Phase,
            attempt.ClaimedAt,
            attempt.StartedAt,
            attempt.LeaseExpiresAt,
            attempt.CompletedAt,
            attempt.FailureCode,
            attempt.FailureMessage)).ToArray());
    }
}
