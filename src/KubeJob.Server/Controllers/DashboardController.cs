using System.Threading.Tasks;
using KubeJob.Server.Data;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers
{
    // Dynamic route applied via convention
    public class DashboardController : Controller
    {
        private readonly IKubeJobRepository _repository;

        public DashboardController(IKubeJobRepository repository)
        {
            _repository = repository;
        }

        [HttpGet("")]
        public async Task<IActionResult> Index()
        {
            var nodes = await _repository.GetAllNodesAsync();
            var recentRuns = await _repository.GetRecentRunsAsync(100); // Fetch more for better stats
            var specs = await _repository.GetAllSpecsAsync();
            
            ViewBag.ActiveNodesCount = nodes.Count(n => !n.IsOffline);
            ViewBag.TotalNodesCount = nodes.Count;
            ViewBag.TotalSpecsCount = specs.Count;
            
            // Basic stats from recent runs
            ViewBag.SuccessCount = recentRuns.Count(r => r.Status == Core.Enums.JobStatus.Succeeded);
            ViewBag.FailedCount = recentRuns.Count(r => r.Status == Core.Enums.JobStatus.Failed);
            ViewBag.PendingCount = recentRuns.Count(r => r.Status == Core.Enums.JobStatus.Pending || r.Status == Core.Enums.JobStatus.Running);

            ViewBag.RecentRuns = recentRuns.Take(15).ToList();
            
            return View("~/Views/Dashboard/Index.cshtml");
        }

        [HttpGet("specs")]
        public async Task<IActionResult> Specs([FromQuery] int page = 1, [FromQuery] int pageSize = 10)
        {
            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = 10;
            if (pageSize > 100) pageSize = 100;

            var totalCount = await _repository.GetSpecsCountAsync();
            var totalPages = (int)System.Math.Ceiling(totalCount / (double)pageSize);
            
            if (page > totalPages && totalPages > 0) page = totalPages;

            var offset = (page - 1) * pageSize;
            var specs = await _repository.GetSpecsPagedAsync(pageSize, offset);

            ViewBag.CurrentPage = page;
            ViewBag.PageSize = pageSize;
            ViewBag.TotalPages = totalPages;
            ViewBag.TotalCount = totalCount;

            return View("~/Views/Dashboard/Specs.cshtml", specs);
        }
        
        [HttpPost("specs/{id}/toggle")]
        [HttpPost("specs/{id}/trigger")]
        public async Task<IActionResult> TriggerSpec(string id)
        {
            var spec = await _repository.GetSpecAsync(id);
            if (spec == null || spec.IsDisabled) return RedirectToAction(nameof(Specs));

            var batchId = $"manual_{System.DateTime.UtcNow:yyyyMMddHHmmss}_{System.Guid.NewGuid().ToString().Substring(0, 4)}";
            for (int i = 0; i < spec.TotalShards; i++)
            {
                var run = new Core.Domain.JobRun
                {
                    Id = System.Guid.NewGuid().ToString().Substring(0, 16),
                    SpecId = spec.Id,
                    BatchId = batchId,
                    ShardIndex = i,
                    Status = Core.Enums.JobStatus.Pending,
                    CreatedAt = System.DateTime.UtcNow
                };
                await _repository.InsertJobRunAsync(run);
            }
            return RedirectToAction(nameof(Runs));
        }

        public async Task<IActionResult> ToggleSpec(string id, [FromForm] bool isDisabled)
        {
            await _repository.UpdateSpecStatusAsync(id, isDisabled);
            return RedirectToAction(nameof(Specs));
        }

        [HttpGet("nodes")]
        public async Task<IActionResult> Nodes()
        {
            var nodes = await _repository.GetAllNodesAsync();
            var sortedNodes = nodes.OrderBy(n => n.IsOffline).ThenByDescending(n => n.LastHeartbeat).ToList();
            
            return View("~/Views/Dashboard/Nodes.cshtml", sortedNodes);
        }

        [HttpPost("nodes/{id}/delete")]
        public async Task<IActionResult> DeleteNode(string id)
        {
            await _repository.DeleteNodeAsync(id);
            return RedirectToAction(nameof(Nodes));
        }

        [HttpGet("runs")]
        public async Task<IActionResult> Runs(int page = 1, int pageSize = 20)
        {
            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = 20;
            if (pageSize > 100) pageSize = 100;
            
            var totalCount = await _repository.GetRunsCountAsync();
            var totalPages = (int)System.Math.Ceiling(totalCount / (double)pageSize);
            
            var runs = await _repository.GetRunsPagedAsync(pageSize, (page - 1) * pageSize);
            
            ViewBag.CurrentPage = page;
            ViewBag.PageSize = pageSize;
            ViewBag.TotalPages = totalPages;
            ViewBag.TotalCount = totalCount;
            
            return View("~/Views/Dashboard/Runs.cshtml", runs);
        }
    }
}
