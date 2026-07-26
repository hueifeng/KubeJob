using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Dashboard;
using KubeJob.Server.Options;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

// The route prefix is applied by KubeJobDashboardRouteConvention.
public sealed class DashboardController : Controller
{
    private readonly IJobRuntimeDashboardStore _dashboard;
    private readonly JobControlPlane _jobs;
    private readonly ScheduleControlPlane _schedules;
    private readonly KubeJobDashboardOptions _options;
    private readonly DashboardCatalogReader _catalog;

    public DashboardController(
        IJobRuntimeDashboardStore dashboard,
        JobControlPlane jobs,
        ScheduleControlPlane schedules,
        KubeJobDashboardOptions options,
        DashboardCatalogReader catalog)
    {
        _dashboard = dashboard;
        _jobs = jobs;
        _schedules = schedules;
        _options = options;
        _catalog = catalog;
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
        bool exactJobKey = false,
        CancellationToken cancellationToken = default)
    {
        var query = new DashboardRunQuery(page, pageSize, phase, queue, jobKey, exactJobKey).Normalize();
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
        bool exactJobKey = false,
        CancellationToken cancellationToken = default)
    {
        var permanentFailureQuery = new DashboardRunQuery(
            failedPage,
            pageSize,
            JobPhase.Failed,
            queue,
            jobKey,
            exactJobKey).Normalize();
        var exhaustedRetryQuery = new DashboardRunQuery(
            deadPage,
            pageSize,
            JobPhase.Dead,
            queue,
            jobKey,
            exactJobKey).Normalize();

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

        var canceled = await _jobs.RequestCancelAsync(
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
    public async Task<IActionResult> Workers(
        bool history = false,
        CancellationToken cancellationToken = default)
    {
        var limit = _options.GetNormalizedMaximumWorkerSessions();
        var allSessions = await _dashboard.GetWorkerSessionsAsync(limit, cancellationToken);
        var activeSessions = allSessions
            .Where(IsActiveWorkerSession)
            .ToArray();
        var sessions = history
            ? allSessions
            : activeSessions;
        return View(
            "~/Views/Dashboard/Workers.cshtml",
            new DashboardWorkersViewModel(
                sessions,
                DateTimeOffset.UtcNow,
                limit,
                history,
                activeSessions.Length,
                allSessions.Count - activeSessions.Length));
    }

    private static bool IsActiveWorkerSession(WorkerSessionRecord session) =>
        session.State is WorkerSessionState.Ready or WorkerSessionState.Draining;

    [HttpGet("job-types")]
    public async Task<IActionResult> JobTypes(CancellationToken cancellationToken)
    {
        var catalog = await _catalog.ReadAsync(cancellationToken);

        return View(
            "~/Views/Dashboard/JobTypes.cshtml",
            new DashboardJobTypesViewModel(
                catalog.JobTypes,
                catalog.RecentRunLimit,
                catalog.ObservedAt));
    }

    [HttpGet("schedules")]
    public async Task<IActionResult> Schedules(CancellationToken cancellationToken)
    {
        var catalog = await _catalog.ReadAsync(cancellationToken);
        return View(
            "~/Views/Dashboard/Schedules.cshtml",
            new DashboardSchedulesViewModel(
                catalog.Schedules,
                _options.AllowMutatingActions,
                _options.GetNormalizedMaximumSchedules(),
                new DashboardScheduleCreateForm(),
                ShowCreateForm: false,
                ReadyJobKeys: catalog.ReadyJobKeys));
    }

    [HttpGet("schedules/preview")]
    public IActionResult PreviewSchedule(string? cronExpression, string? timeZoneId)
    {
        try
        {
            var preview = _schedules.PreviewCron(
                cronExpression?.Trim() ?? string.Empty,
                timeZoneId?.Trim() ?? string.Empty,
                DateTimeOffset.UtcNow,
                3);
            var timeZone = TimeZoneInfo.FindSystemTimeZoneById(preview.TimeZoneId);
            var occurrences = preview.Occurrences
                .Select(occurrence => new
                {
                    Iso = occurrence.ToUniversalTime().ToString("O"),
                    Display = TimeZoneInfo.ConvertTime(occurrence, timeZone)
                        .ToString("ddd, MMM d · HH:mm zzz")
                });
            return Ok(new { timeZoneId = preview.TimeZoneId, occurrences });
        }
        catch (ControlPlaneValidationException validation)
        {
            return BadRequest(new { message = validation.Message });
        }
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

        if (!Enum.IsDefined(form.MisfirePolicy))
        {
            ModelState.AddModelError("CreateForm.MisfirePolicy", "Choose a supported missed-run behavior.");
        }

        if (!Enum.IsDefined(form.ConcurrencyPolicy))
        {
            ModelState.AddModelError("CreateForm.ConcurrencyPolicy", "Choose a supported overlap behavior.");
        }

        try
        {
            _schedules.PreviewCron(
                form.CronExpression,
                form.TimeZoneId,
                DateTimeOffset.UtcNow,
                1);
        }
        catch (ControlPlaneValidationException validation)
        {
            ModelState.AddModelError(
                "CreateForm.CronExpression",
                validation.Message);
        }

        if (!ModelState.IsValid)
        {
            return await RenderScheduleFormAsync(form, cancellationToken);
        }

        var request = new UpsertCronScheduleRequest(
            form.JobKey,
            form.PayloadJson,
            form.CronExpression,
            form.TimeZoneId,
            form.Queue,
            form.Priority,
            form.MisfirePolicy,
            form.ConcurrencyPolicy,
            form.MaxAttempts,
            form.TimeoutSeconds,
            form.Enabled);
        JobScheduleSnapshot? schedule;
        try
        {
            schedule = await _schedules.CreateCronAsync(
                form.Id,
                request,
                cancellationToken);
        }
        catch (ControlPlaneValidationException validation)
        {
            ModelState.AddModelError(string.Empty, validation.Message);
            return await RenderScheduleFormAsync(form, cancellationToken);
        }

        if (schedule is null)
        {
            ModelState.AddModelError("CreateForm.Id", "A Schedule with this ID already exists.");
            return await RenderScheduleFormAsync(form, cancellationToken);
        }

        return RedirectToAction(nameof(Schedules));
    }

    private async Task<IActionResult> RenderScheduleFormAsync(
        DashboardScheduleCreateForm form,
        CancellationToken cancellationToken)
    {
        var catalog = await _catalog.ReadAsync(cancellationToken);
        return View(
            "~/Views/Dashboard/Schedules.cshtml",
            new DashboardSchedulesViewModel(
                catalog.Schedules,
                _options.AllowMutatingActions,
                _options.GetNormalizedMaximumSchedules(),
                form,
                ShowCreateForm: true,
                ReadyJobKeys: catalog.ReadyJobKeys));
    }

    [HttpPost("schedules/{id}/enabled")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SetScheduleEnabled(
        string id,
        [FromForm] bool enabled,
        [FromForm] long expectedVersion,
        CancellationToken cancellationToken)
    {
        if (!_options.AllowMutatingActions)
        {
            return Forbid();
        }

        var updated = await _schedules.SetEnabledAsync(
            id,
            enabled,
            expectedVersion,
            cancellationToken);
        if (!updated)
        {
            return await _schedules.GetAsync(id, cancellationToken) is null
                ? NotFound()
                : Conflict("The Schedule changed concurrently. Refresh and try again.");
        }

        return RedirectToAction(nameof(Schedules));
    }

    [HttpPost("schedules/{id}/delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteSchedule(
        string id,
        [FromForm] long expectedVersion,
        CancellationToken cancellationToken)
    {
        if (!_options.AllowMutatingActions)
        {
            return Forbid();
        }

        var deleted = await _schedules.DeleteAsync(
            id,
            expectedVersion,
            cancellationToken);
        if (deleted)
        {
            return RedirectToAction(nameof(Schedules));
        }

        return await _schedules.GetAsync(id, cancellationToken) is null
            ? NotFound()
            : Conflict("The Schedule changed concurrently. Refresh and try again.");
    }
}
