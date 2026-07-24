using KubeJob.Core.Client;
using KubeJob.Server.Dashboard;
using KubeJob.Server.Options;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers;

// The route prefix is applied by KubeJobDashboardRouteConvention.
public sealed class DashboardController : Controller
{
    private readonly IJobRuntimeDashboardStore _dashboard;
    private readonly IJobQueryStore _queries;
    private readonly IJobSubmissionStore _submissions;
    private readonly IJobScheduleStore _schedules;
    private readonly KubeJobDashboardOptions _options;

    public DashboardController(
        IJobRuntimeDashboardStore dashboard,
        IJobQueryStore queries,
        IJobSubmissionStore submissions,
        IJobScheduleStore schedules,
        KubeJobDashboardOptions options)
    {
        _dashboard = dashboard;
        _queries = queries;
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
        return View(
            "~/Views/Dashboard/Runs.cshtml",
            new DashboardRunsViewModel(runs, query));
    }

    [HttpGet("runs/{id}")]
    public async Task<IActionResult> Run(
        string id,
        CancellationToken cancellationToken)
    {
        var run = await _queries.GetRunAsync(id, cancellationToken);
        if (run is null)
        {
            return NotFound();
        }

        var attempts = await _queries.GetAttemptsAsync(id, cancellationToken);
        return View(
            "~/Views/Dashboard/Run.cshtml",
            new DashboardRunDetailsViewModel(
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
            var existing = await _queries.GetRunAsync(id, cancellationToken);
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
                limit));
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
}
