using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/jobs")]
public sealed class JobsApiController : ControllerBase
{
    private readonly IJobSubmissionStore _submissions;
    private readonly IJobQueryStore _queries;

    public JobsApiController(
        IJobSubmissionStore submissions,
        IJobQueryStore queries)
    {
        _submissions = submissions;
        _queries = queries;
    }

    [HttpPost]
    public async Task<ActionResult<JobHandle>> Enqueue(
        [FromBody] EnqueueJobRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.JobKey)
            || string.IsNullOrWhiteSpace(request.PayloadJson)
            || string.IsNullOrWhiteSpace(request.Queue)
            || request.MaxAttempts < 1
            || request.TimeoutSeconds < 1)
        {
            return BadRequest(
                "JobKey, valid payload JSON, queue, positive MaxAttempts, and positive TimeoutSeconds are required.");
        }

        try
        {
            using var document = JsonDocument.Parse(request.PayloadJson);
        }
        catch (JsonException)
        {
            return BadRequest("PayloadJson must contain valid JSON.");
        }

        var result = await _submissions.SubmitAsync(
            new SubmitJobCommand(
                request.JobKey,
                request.PayloadJson,
                request.Queue,
                request.Priority,
                (request.NotBefore ?? DateTimeOffset.UtcNow).ToUniversalTime(),
                request.IdempotencyKey,
                request.ConcurrencyKey,
                request.MaxAttempts,
                request.TimeoutSeconds),
            cancellationToken);

        var handle = new JobHandle(result.Run.Id);
        return result.Existing ? Ok(handle) : Accepted(handle);
    }

    [HttpGet("{runId}")]
    public async Task<ActionResult<JobStatusSnapshot>> GetStatus(
        string runId,
        CancellationToken cancellationToken)
    {
        var run = await _queries.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            return NotFound();
        }

        return Ok(ToSnapshot(run));
    }

    [HttpGet("{runId}/attempts")]
    public async Task<ActionResult<IReadOnlyList<JobAttemptRecord>>> GetAttempts(
        string runId,
        CancellationToken cancellationToken)
    {
        var run = await _queries.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            return NotFound();
        }

        return Ok(await _queries.GetAttemptsAsync(runId, cancellationToken));
    }

    [HttpPost("{runId}/cancel")]
    public async Task<IActionResult> Cancel(
        string runId,
        [FromBody] CancelJobRequest? request,
        CancellationToken cancellationToken)
    {
        var accepted = await _submissions.RequestCancelAsync(
            runId,
            request?.Reason,
            cancellationToken);
        return accepted ? Accepted() : NotFound();
    }

    private static JobStatusSnapshot ToSnapshot(JobRunRecord run) => new(
        run.Id,
        run.Phase,
        run.AttemptCount,
        run.CreatedAt,
        run.StartedAt,
        run.CompletedAt,
        run.CurrentWorkerId,
        run.FailureCode,
        run.FailureMessage);
}
