using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KubeJob.Core.Enums;
using KubeJob.Server.Data;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers
{
    // Dynamic route applied via convention
    [AutoValidateAntiforgeryToken]
    public class DashboardController : Controller
    {
        private const int HistogramHours = 24;

        private static readonly JobStatus[] TabOrder =
        {
            JobStatus.Pending,
            JobStatus.Assigned,
            JobStatus.Running,
            JobStatus.Succeeded,
            JobStatus.Failed,
            JobStatus.Canceled
        };

        private readonly IKubeJobRepository _repository;

        public DashboardController(IKubeJobRepository repository)
        {
            _repository = repository;
        }

        [HttpGet("")]
        public async Task<IActionResult> Index()
        {
            var nodes = await _repository.GetAllNodesAsync();
            var specs = await _repository.GetAllSpecsAsync();
            var statusCounts = await _repository.GetRunStatusCountsAsync();
            var recentRuns = await _repository.GetRecentRunsAsync(10);
            var histogram = await BuildHistogramAsync();

            ViewBag.ActiveNodesCount = nodes.Count(n => !n.IsOffline);
            ViewBag.TotalNodesCount = nodes.Count;
            ViewBag.OfflineNodesCount = nodes.Count(n => n.IsOffline);
            ViewBag.TotalSpecsCount = specs.Count;
            ViewBag.PausedSpecsCount = specs.Count(s => s.IsDisabled);

            ViewBag.StatusCounts = TabOrder.ToDictionary(s => s, s => Count(statusCounts, s));
            ViewBag.RecentRuns = recentRuns;
            ViewBag.Histogram = histogram;

            return View("~/Views/Dashboard/Index.cshtml");
        }

        [HttpGet("api/overview")]
        public async Task<IActionResult> OverviewData()
        {
            var nodes = await _repository.GetAllNodesAsync();
            var specs = await _repository.GetAllSpecsAsync();
            var statusCounts = await _repository.GetRunStatusCountsAsync();
            var recentRuns = await _repository.GetRecentRunsAsync(10);
            var histogram = await BuildHistogramAsync();

            return Ok(new
            {
                generatedAt = DateTime.UtcNow,
                stats = new
                {
                    activeNodes = nodes.Count(n => !n.IsOffline),
                    totalNodes = nodes.Count,
                    offlineNodes = nodes.Count(n => n.IsOffline),
                    totalSpecs = specs.Count,
                    pausedSpecs = specs.Count(s => s.IsDisabled),
                    pending = Count(statusCounts, JobStatus.Pending),
                    assigned = Count(statusCounts, JobStatus.Assigned),
                    running = Count(statusCounts, JobStatus.Running),
                    succeeded = Count(statusCounts, JobStatus.Succeeded),
                    failed = Count(statusCounts, JobStatus.Failed),
                    canceled = Count(statusCounts, JobStatus.Canceled)
                },
                histogram = histogram.Select(b => new
                {
                    hourUtc = b.HourUtc,
                    succeeded = b.Succeeded,
                    failed = b.Failed
                }),
                recentRuns = recentRuns.Select(ToRunDto)
            });
        }

        [HttpGet("specs")]
        public async Task<IActionResult> Specs([FromQuery] int page = 1, [FromQuery] int pageSize = 20)
        {
            (page, pageSize) = Normalize(page, pageSize, 20);

            var totalCount = await _repository.GetSpecsCountAsync();
            var totalPages = (int)Math.Ceiling(totalCount / (double)pageSize);
            if (page > totalPages && totalPages > 0) page = totalPages;

            var specs = await _repository.GetSpecsPagedAsync(pageSize, (page - 1) * pageSize);

            ViewBag.CurrentPage = page;
            ViewBag.PageSize = pageSize;
            ViewBag.TotalPages = totalPages;
            ViewBag.TotalCount = totalCount;

            return View("~/Views/Dashboard/Specs.cshtml", specs);
        }

        [HttpPost("specs/{id}/trigger")]
        public async Task<IActionResult> TriggerSpec(string id)
        {
            var spec = await _repository.GetSpecAsync(id);
            if (spec == null || spec.IsDisabled) return RedirectToAction(nameof(Specs));

            var batchId = $"manual_{DateTime.UtcNow:yyyyMMddHHmmss}_{Guid.NewGuid().ToString("N").Substring(0, 4)}";
            var totalShards = Math.Max(1, spec.TotalShards);

            for (int i = 0; i < totalShards; i++)
            {
                await _repository.InsertJobRunAsync(new Core.Domain.JobRun
                {
                    Id = Guid.NewGuid().ToString("N").Substring(0, 16),
                    SpecId = spec.Id,
                    BatchId = batchId,
                    ShardIndex = i,
                    Status = JobStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                });
            }

            return RedirectToAction(nameof(Runs));
        }

        [HttpPost("specs/{id}/toggle")]
        public async Task<IActionResult> ToggleSpec(string id, [FromForm] bool isDisabled)
        {
            await _repository.UpdateSpecStatusAsync(id, isDisabled);
            return RedirectToAction(nameof(Specs));
        }

        [HttpGet("nodes")]
        public async Task<IActionResult> Nodes()
        {
            var nodes = await _repository.GetAllNodesAsync();
            return View("~/Views/Dashboard/Nodes.cshtml", SortNodes(nodes));
        }

        [HttpGet("api/nodes")]
        public async Task<IActionResult> NodesData()
        {
            var nodes = await _repository.GetAllNodesAsync();
            return Ok(SortNodes(nodes).Select(n => new
            {
                id = n.Id,
                ipAddress = n.IpAddress,
                labels = n.Labels,
                lastHeartbeat = n.LastHeartbeat,
                currentLoad = n.CurrentLoad,
                maxCapacity = n.MaxCapacity,
                isOffline = n.IsOffline
            }));
        }

        [HttpPost("nodes/{id}/delete")]
        public async Task<IActionResult> DeleteNode(string id)
        {
            await _repository.DeleteNodeAsync(id);
            return RedirectToAction(nameof(Nodes));
        }

        [HttpGet("runs")]
        public async Task<IActionResult> Runs(string? status = null, int page = 1, int pageSize = 20)
        {
            (page, pageSize) = Normalize(page, pageSize, 20);
            var filter = ParseStatus(status);

            var statusCounts = await _repository.GetRunStatusCountsAsync();
            var totalCount = filter.HasValue ? Count(statusCounts, filter.Value) : statusCounts.Values.Sum();

            var totalPages = (int)Math.Ceiling(totalCount / (double)pageSize);
            if (page > totalPages && totalPages > 0) page = totalPages;

            var runs = await _repository.GetRunsPagedAsync(pageSize, (page - 1) * pageSize, filter);

            ViewBag.CurrentPage = page;
            ViewBag.PageSize = pageSize;
            ViewBag.TotalPages = totalPages;
            ViewBag.TotalCount = totalCount;
            ViewBag.ActiveStatus = filter;
            ViewBag.AllRunsCount = statusCounts.Values.Sum();
            ViewBag.StatusCounts = TabOrder.ToDictionary(s => s, s => Count(statusCounts, s));
            ViewBag.TabOrder = TabOrder;

            return View("~/Views/Dashboard/Runs.cshtml", runs);
        }

        [HttpGet("api/runs")]
        public async Task<IActionResult> RunsData([FromQuery] string? status = null, [FromQuery] int limit = 20, [FromQuery] int page = 1)
        {
            if (limit < 1) limit = 1;
            if (limit > 100) limit = 100;
            if (page < 1) page = 1;

            var filter = ParseStatus(status);
            var statusCounts = await _repository.GetRunStatusCountsAsync();
            var runs = await _repository.GetRunsPagedAsync(limit, (page - 1) * limit, filter);

            return Ok(new
            {
                generatedAt = DateTime.UtcNow,
                counts = new
                {
                    all = statusCounts.Values.Sum(),
                    pending = Count(statusCounts, JobStatus.Pending),
                    assigned = Count(statusCounts, JobStatus.Assigned),
                    running = Count(statusCounts, JobStatus.Running),
                    succeeded = Count(statusCounts, JobStatus.Succeeded),
                    failed = Count(statusCounts, JobStatus.Failed),
                    canceled = Count(statusCounts, JobStatus.Canceled)
                },
                runs = runs.Select(ToRunDto)
            });
        }

        private static (int page, int pageSize) Normalize(int page, int pageSize, int defaultPageSize)
        {
            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = defaultPageSize;
            if (pageSize > 100) pageSize = 100;
            return (page, pageSize);
        }

        private static int Count(IReadOnlyDictionary<JobStatus, int> counts, JobStatus status)
            => counts.TryGetValue(status, out var value) ? value : 0;

        private static JobStatus? ParseStatus(string? status)
            => Enum.TryParse<JobStatus>(status, ignoreCase: true, out var parsed) ? parsed : null;

        private static List<Core.Domain.WorkerNode> SortNodes(IEnumerable<Core.Domain.WorkerNode> nodes)
            => nodes.OrderBy(n => n.IsOffline).ThenByDescending(n => n.LastHeartbeat).ToList();

        private static object ToRunDto(Core.Domain.JobRun run) => new
        {
            id = run.Id,
            specId = run.SpecId,
            batchId = run.BatchId,
            shardIndex = run.ShardIndex,
            targetNodeId = run.TargetNodeId,
            status = run.Status.ToString(),
            createdAt = run.CreatedAt,
            startTime = run.StartTime,
            endTime = run.EndTime,
            durationSeconds = run.EndTime.HasValue && run.StartTime.HasValue
                ? Math.Round((run.EndTime.Value - run.StartTime.Value).TotalSeconds, 1)
                : (double?)null,
            resultMsg = run.ResultMsg
        };

        /// <summary>
        /// Builds a dense 24-hour succeeded/failed series so the graph always renders a
        /// continuous timeline even when some hours have no activity.
        /// </summary>
        private async Task<List<HistogramPoint>> BuildHistogramAsync()
        {
            var now = DateTime.UtcNow;
            var currentHour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc);
            var since = currentHour.AddHours(-(HistogramHours - 1));

            var raw = await _repository.GetRunHistogramAsync(since);
            var lookup = raw.ToLookup(b => DateTime.SpecifyKind(b.BucketUtc, DateTimeKind.Utc));

            var points = new List<HistogramPoint>(HistogramHours);
            for (int i = 0; i < HistogramHours; i++)
            {
                var hour = since.AddHours(i);
                var bucket = lookup[hour].ToList();
                points.Add(new HistogramPoint
                {
                    HourUtc = hour,
                    Succeeded = bucket.Where(b => b.Status == JobStatus.Succeeded).Sum(b => b.Count),
                    Failed = bucket.Where(b => b.Status == JobStatus.Failed).Sum(b => b.Count)
                });
            }

            return points;
        }

        public class HistogramPoint
        {
            public DateTime HourUtc { get; set; }
            public int Succeeded { get; set; }
            public int Failed { get; set; }
        }
    }
}
