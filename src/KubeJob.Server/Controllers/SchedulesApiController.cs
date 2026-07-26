using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.ControlPlane;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/schedules")]
public sealed class SchedulesApiController : ControllerBase
{
    private readonly ScheduleControlPlane _controlPlane;

    public SchedulesApiController(ScheduleControlPlane controlPlane)
    {
        _controlPlane = controlPlane;
    }

    [HttpPut("{scheduleId}")]
    public async Task<ActionResult<JobScheduleSnapshot>> Upsert(
        string scheduleId,
        [FromBody] UpsertCronScheduleRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            var schedule = await _controlPlane.UpsertCronAsync(
                scheduleId,
                request,
                cancellationToken);
            return Ok(schedule);
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

    [HttpGet("{scheduleId}")]
    public async Task<ActionResult<JobScheduleSnapshot>> Get(
        string scheduleId,
        CancellationToken cancellationToken)
    {
        var schedule = await _controlPlane.GetAsync(scheduleId, cancellationToken);
        return schedule is null
            ? NotFound()
            : Ok(schedule);
    }

    [HttpPost("{scheduleId}/enabled")]
    public async Task<IActionResult> SetEnabled(
        string scheduleId,
        [FromBody] SetScheduleEnabledRequest request,
        CancellationToken cancellationToken)
    {
        var updated = await _controlPlane.SetEnabledAsync(
            scheduleId,
            request.Enabled,
            cancellationToken: cancellationToken);
        return updated ? Ok() : NotFound();
    }

    [HttpDelete("{scheduleId}")]
    public async Task<IActionResult> Delete(
        string scheduleId,
        CancellationToken cancellationToken)
    {
        return await _controlPlane.DeleteAsync(
                scheduleId,
                cancellationToken: cancellationToken)
            ? NoContent()
            : NotFound();
    }
}
