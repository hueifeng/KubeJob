using System.Text.Json;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

[ApiController]
[Route("api/kubejob/schedules")]
public sealed class SchedulesApiController : ControllerBase
{
    private readonly IJobScheduleStore _store;

    public SchedulesApiController(IJobScheduleStore store)
    {
        _store = store;
    }

    [HttpPut("{scheduleId}")]
    public async Task<ActionResult<JobScheduleSnapshot>> Upsert(
        string scheduleId,
        [FromBody] UpsertCronScheduleRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(scheduleId)
            || string.IsNullOrWhiteSpace(request.JobKey)
            || string.IsNullOrWhiteSpace(request.PayloadJson)
            || string.IsNullOrWhiteSpace(request.CronExpression)
            || string.IsNullOrWhiteSpace(request.TimeZoneId)
            || string.IsNullOrWhiteSpace(request.Queue)
            || request.MaxAttempts < 1
            || request.TimeoutSeconds < 1)
        {
            return BadRequest("Schedule id, job key, payload, cron, time zone, queue, and positive limits are required.");
        }

        try
        {
            using var payload = JsonDocument.Parse(request.PayloadJson);
            CronScheduleCalculator.Validate(request.CronExpression, request.TimeZoneId);
        }
        catch (Exception ex) when (ex is JsonException or Cronos.CronFormatException or TimeZoneNotFoundException or InvalidTimeZoneException or InvalidOperationException)
        {
            return BadRequest(new { code = "invalid_schedule", message = ex.Message });
        }

        var now = DateTimeOffset.UtcNow;
        var nextFireAt = CronScheduleCalculator.GetRequiredNextOccurrence(
            request.CronExpression,
            request.TimeZoneId,
            now);
        var schedule = await _store.UpsertAsync(new JobScheduleRecord
        {
            Id = scheduleId,
            JobKey = request.JobKey,
            PayloadJson = request.PayloadJson,
            CronExpression = request.CronExpression,
            TimeZoneId = request.TimeZoneId,
            Queue = request.Queue,
            Priority = request.Priority,
            MisfirePolicy = request.MisfirePolicy,
            ConcurrencyPolicy = request.ConcurrencyPolicy,
            MaxAttempts = request.MaxAttempts,
            TimeoutSeconds = request.TimeoutSeconds,
            Enabled = request.Enabled,
            NextFireAt = nextFireAt.ToUniversalTime(),
            CreatedAt = now,
            UpdatedAt = now
        }, cancellationToken);

        return Ok(DefaultJobScheduleClient.ToSnapshot(schedule));
    }

    [HttpGet("{scheduleId}")]
    public async Task<ActionResult<JobScheduleSnapshot>> Get(
        string scheduleId,
        CancellationToken cancellationToken)
    {
        var schedule = await _store.GetAsync(scheduleId, cancellationToken);
        return schedule is null
            ? NotFound()
            : Ok(DefaultJobScheduleClient.ToSnapshot(schedule));
    }

    [HttpPost("{scheduleId}/enabled")]
    public async Task<IActionResult> SetEnabled(
        string scheduleId,
        [FromBody] SetScheduleEnabledRequest request,
        CancellationToken cancellationToken)
    {
        var schedule = await _store.GetAsync(scheduleId, cancellationToken);
        if (schedule is null)
        {
            return NotFound();
        }

        DateTimeOffset? nextFireAt = null;
        if (request.Enabled)
        {
            nextFireAt = CronScheduleCalculator.GetRequiredNextOccurrence(
                schedule.CronExpression,
                schedule.TimeZoneId,
                DateTimeOffset.UtcNow);
        }

        var updated = await _store.SetEnabledAsync(
            scheduleId,
            request.Enabled,
            nextFireAt,
            cancellationToken);
        return updated ? Ok() : NotFound();
    }

    [HttpDelete("{scheduleId}")]
    public async Task<IActionResult> Delete(
        string scheduleId,
        CancellationToken cancellationToken)
    {
        return await _store.DeleteAsync(scheduleId, cancellationToken)
            ? NoContent()
            : NotFound();
    }
}
