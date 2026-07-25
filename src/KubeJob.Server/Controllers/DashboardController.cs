using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Dashboard;
using KubeJob.Server.Options;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

// The route prefix is applied by KubeJobDashboardRouteConvention.
public sealed class DashboardController : Controller
{
    private readonly IJobRuntimeDashboardStore _dashboard;
    private readonly IJobSubmissionStore _submissions;
    private readonly IJobScheduleStore _schedules;
    private readonly KubeJobDashboardOptions _options;

    public DashboardController(
        IJobRuntimeDashboardStore dashboard,
        IJobSubmissionStore submissions,
        IJobScheduleStore schedules,
        KubeJobDashboardOptions options)
    {
        _dashboard = dashboard;
        _submissions = submissions;
        _schedules = schedules;
        _options = options;
    }

    [HttpGet("")]
    public async Task<IActionResult> Index(CancellationToken cancellationToken)
    {
        var overview = await _dashboard.GetOverviewAsync(15, cancellationToken);
        return View(
            "~/Views/Dashboard/Index.cshtml",
            new DashboardIndexViewModel(overview));
    }

    [HttpGet("runs")]
    public async Task<IActionResult> Runs(
        int page = 1,
        int pageSize = 25,
        JobPhase? phase = null,
        string? queue = null,
        string? jobKey = null,
        CancellationToken cancellationToken = default)
    {
        var query = new DashboardRunQuery(page, pageSize, phase, queue, jobKey).Normalize();
        var runs = await _dashboard.GetRunsAsync(query, cancellationToken);
        var overview = await _dashboard.GetOverviewAsync(1, cancellationToken);
        return View(
            "~/Views/Dashboard/Runs.cshtml",
            new DashboardRunsViewModel(runs, query, overview));
    }

    [HttpGet("failures")]
    public async Task<IActionResult> Failures(
        int failedPage = 1,
        int deadPage = 1,
        int pageSize = 25,
        string? queue = null,
        string? jobKey = null,
        CancellationToken cancellationToken = default)
    {
        var permanentFailureQuery = new DashboardRunQuery(
            failedPage,
            pageSize,
            JobPhase.Failed,
            queue,
            jobKey).Normalize();
        var exhaustedRetryQuery = new DashboardRunQuery(
            deadPage,
            pageSize,
            JobPhase.Dead,
            queue,
            jobKey).Normalize();

        var permanentFailures = await _dashboard.GetRunsAsync(
            permanentFailureQuery,
            cancellationToken);
        var exhaustedRetries = await _dashboard.GetRunsAsync(
            exhaustedRetryQuery,
            cancellationToken);
        var overview = await _dashboard.GetOverviewAsync(1, cancellationToken);

        return View(
            "~/Views/Dashboard/Failures.cshtml",
            new DashboardFailuresViewModel(
                permanentFailures,
                exhaustedRetries,
                permanentFailureQuery,
                exhaustedRetryQuery,
                overview));
    }

    [HttpGet("runs/{id}")]
    public async Task<IActionResult> Run(
        string id,
        CancellationToken cancellationToken)
    {
        var run = await _dashboard.GetRunDetailsAsync(
            id,
            _options.ShowPayloads,
            cancellationToken);
        if (run is null)
        {
            return NotFound();
        }

        var attempts = await _dashboard.GetAttemptSummariesAsync(id, cancellationToken);
        return View(
            "~/Views/Dashboard/Run.cshtml",
            DashboardRunDetailsViewModel.Create(
                run,
                attempts,
                _options.ShowPayloads,
                _options.AllowMutatingActions));
    }

    [HttpPost("runs/{id}/cancel")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> CancelRun(
        string id,
        [FromForm] string? reason,
        CancellationToken cancellationToken)
    {
        if (!_options.AllowMutatingActions)
        {
            return Forbid();
        }

        var canceled = await _submissions.RequestCancelAsync(
            id,
            string.IsNullOrWhiteSpace(reason) ? "Canceled from dashboard." : reason.Trim(),
            cancellationToken);
        if (!canceled)
        {
            var existing = await _dashboard.GetRunDetailsAsync(
                id,
                includePayload: false,
                cancellationToken);
            return existing is null
                ? NotFound()
                : Conflict("The Run is already terminal and cannot be canceled.");
        }

        return RedirectToAction(nameof(Run), new { id });
    }

    [HttpGet("workers")]
    public async Task<IActionResult> Workers(CancellationToken cancellationToken)
    {
        var limit = _options.GetNormalizedMaximumWorkerSessions();
        var sessions = await _dashboard.GetWorkerSessionsAsync(limit, cancellationToken);
        return View(
            "~/Views/Dashboard/Workers.cshtml",
            new DashboardWorkersViewModel(sessions, DateTimeOffset.UtcNow, limit));
    }

    [HttpGet("schedules")]
    public async Task<IActionResult> Schedules(CancellationToken cancellationToken)
    {
        var limit = _options.GetNormalizedMaximumSchedules();
        var schedules = await _dashboard.GetSchedulesAsync(limit, cancellationToken);
        return View(
            "~/Views/Dashboard/Schedules.cshtml",
            new DashboardSchedulesViewModel(
                schedules,
                _options.AllowMutatingActions,
                limit,
                new DashboardScheduleCreateForm()));
    }

    [HttpPost("schedules/create")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> CreateSchedule(
        [Bind(Prefix = "CreateForm")] DashboardScheduleCreateForm form,
        CancellationToken cancellationToken)
    {
        if (!_options.AllowMutatingActions)
        {
            return Forbid();
        }

        form.Id = form.Id?.Trim() ?? string.Empty;
        form.JobKey = form.JobKey?.Trim() ?? string.Empty;
        form.PayloadJson = form.PayloadJson?.Trim() ?? string.Empty;
        form.CronExpression = form.CronExpression?.Trim() ?? string.Empty;
        form.TimeZoneId = form.TimeZoneId?.Trim() ?? string.Empty;
        form.Queue = form.Queue?.Trim() ?? string.Empty;

        if (!string.IsNullOrWhiteSpace(form.Id)
            && await _schedules.GetAsync(form.Id, cancellationToken) is not null)
        {
            ModelState.AddModelError("CreateForm.Id", "A Schedule with this ID already exists.");
        }

        if (!string.IsNullOrWhiteSpace(form.PayloadJson))
        {
            try
            {
                using var payload = JsonDocument.Parse(form.PayloadJson);
            }
            catch (JsonException)
            {
                ModelState.AddModelError("CreateForm.PayloadJson", "Payload must be valid JSON.");
            }
        }

        try
        {
            CronScheduleCalculator.Validate(form.CronExpression, form.TimeZoneId);
        }
        catch (Exception ex) when (ex is Cronos.CronFormatException
                                   or TimeZoneNotFoundException
                                   or InvalidTimeZoneException
                                   or InvalidOperationException
                                   or ArgumentException)
        {
            ModelState.AddModelError("CreateForm.CronExpression", ex.Message);
        }

        if (!ModelState.IsValid)
        {
            var limit = _options.GetNormalizedMaximumSchedules();
            var schedules = await _dashboard.GetSchedulesAsync(limit, cancellationToken);
            return View(
                "~/Views/Dashboard/Schedules.cshtml",
                new DashboardSchedulesViewModel(
                    schedules,
                    _options.AllowMutatingActions,
                    limit,
                    form));
        }

        var now = DateTimeOffset.UtcNow;
        var schedule = new JobScheduleRecord
        {
            Id = form.Id.Trim(),
            JobKey = form.JobKey.Trim(),
            PayloadJson = form.PayloadJson.Trim(),
            CronExpression = form.CronExpression.Trim(),
            TimeZoneId = form.TimeZoneId.Trim(),
            Queue = form.Queue.Trim(),
            Priority = form.Priority,
            MisfirePolicy = form.MisfirePolicy,
            ConcurrencyPolicy = form.ConcurrencyPolicy,
            MaxAttempts = form.MaxAttempts,
            TimeoutSeconds = form.TimeoutSeconds,
            Enabled = form.Enabled,
            NextFireAt = CronScheduleCalculator.GetRequiredNextOccurrence(
                form.CronExpression,
                form.TimeZoneId,
                now).ToUniversalTime(),
            CreatedAt = now,
            UpdatedAt = now
        };

        await _schedules.UpsertAsync(schedule, cancellationToken);
        return RedirectToAction(nameof(Schedules));
    }

    [HttpPost("schedules/{id}/enabled")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SetScheduleEnabled(
        string id,
        [FromForm] bool enabled,
        CancellationToken cancellationToken)
    {
        if (!_options.AllowMutatingActions)
        {
            return Forbid();
        }

        var schedule = await _schedules.GetAsync(id, cancellationToken);
        if (schedule is null)
        {
            return NotFound();
        }

        DateTimeOffset? nextFireAt = null;
        if (enabled)
        {
            nextFireAt = CronScheduleCalculator.GetRequiredNextOccurrence(
                schedule.CronExpression,
                schedule.TimeZoneId,
                DateTimeOffset.UtcNow);
        }

        var updated = await _schedules.SetEnabledAsync(
            id,
            enabled,
            nextFireAt,
            cancellationToken);
        if (!updated)
        {
            return Conflict("The Schedule changed concurrently. Refresh and try again.");
        }

        return RedirectToAction(nameof(Schedules));
    }

    [HttpPost("schedules/{id}/delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteSchedule(
        string id,
        CancellationToken cancellationToken)
    {
        if (!_options.AllowMutatingActions)
        {
            return Forbid();
        }

        var deleted = await _schedules.DeleteAsync(id, cancellationToken);
        return deleted
            ? RedirectToAction(nameof(Schedules))
            : NotFound();
    }
}
