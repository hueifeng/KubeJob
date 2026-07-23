using System;
using System.Text.Json;
using System.Threading.Tasks;
using KubeJob.Core.Domain;
using KubeJob.Core.Dtos;
using KubeJob.Server.Data;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers
{
    [ApiController]
    [Route("api/kubejob/worker")]
    public class WorkerApiController : ControllerBase
    {
        private readonly IKubeJobRepository _repository;

        public WorkerApiController(IKubeJobRepository repository)
        {
            _repository = repository;
        }

        [HttpPost("register")]
        public async Task<IActionResult> RegisterJobs([FromBody] RegisterJobsRequest request)
        {
            if (request == null || request.Jobs == null)
            {
                return BadRequest("Invalid registration request");
            }

            foreach (var jobDto in request.Jobs)
            {
                var existingSpec = await _repository.GetSpecByNameAsync(jobDto.Name);
                if (existingSpec == null)
                {
                    var newSpec = new JobSpec
                    {
                        Id = Guid.NewGuid().ToString(),
                        Name = jobDto.Name,
                        JobType = jobDto.Name,
                        CronExpression = jobDto.Cron,
                        ExecuteModel = jobDto.ExecuteModel,
                        TotalShards = jobDto.TotalShards,
                        TimeoutSeconds = jobDto.TimeoutSeconds,
                        MaxRetries = jobDto.MaxRetries,
                        NodeSelector = JsonSerializer.Serialize(jobDto.NodeSelectors),
                        ConcurrencyPolicy = KubeJob.Core.Enums.ConcurrencyPolicy.Allow, // Default policy
                        IsDisabled = false
                    };
                    await _repository.UpsertSpecAsync(newSpec);
                }
                else
                {
                    // Update existing JobSpec with code changes
                    existingSpec.JobType = jobDto.Name; // Keep synchronized
                    existingSpec.CronExpression = jobDto.Cron;
                    existingSpec.ExecuteModel = jobDto.ExecuteModel;
                    existingSpec.TotalShards = jobDto.TotalShards;
                    existingSpec.TimeoutSeconds = jobDto.TimeoutSeconds;
                    existingSpec.MaxRetries = jobDto.MaxRetries;
                    existingSpec.NodeSelector = JsonSerializer.Serialize(jobDto.NodeSelectors);
                    await _repository.UpsertSpecAsync(existingSpec);
                }
            }

            return Ok(new { Count = request.Jobs.Count });
        }

        [HttpPost("heartbeat")]
        public async Task<IActionResult> Heartbeat([FromBody] HeartbeatRequest request)
        {
            var node = new WorkerNode
            {
                Id = request.WorkerId,
                IpAddress = request.IpAddress ?? "Unknown",
                Labels = JsonSerializer.Serialize(request.Labels),
                LastHeartbeat = DateTime.UtcNow,
                CurrentLoad = request.CurrentLoad,
                MaxCapacity = request.MaxCapacity,
                IsOffline = false
            };
            
            await _repository.UpsertWorkerNodeAsync(node);
            return Ok();
        }

        [HttpGet("poll")]
        public async Task<IActionResult> Poll([FromQuery] string workerId, [FromQuery] int maxJobs = 10)
        {
            maxJobs = Math.Clamp(maxJobs, 1, 1000);
            // Simple long-polling simulation (wait up to 5 seconds if no jobs)
            for (int i = 0; i < 5; i++)
            {
                var runs = await _repository.GetAssignedRunsForNodeAsync(workerId, maxJobs);
                if (runs.Count > 0)
                {
                    return Ok(new PollJobsResponse { Jobs = runs });
                }
                await Task.Delay(1000);
            }
            
            return Ok(new PollJobsResponse { Jobs = new() });
        }

        [HttpPost("report")]
        public async Task<IActionResult> Report([FromBody] JobReportRequest request)
        {
            DateTime? startTime = request.Status == KubeJob.Core.Enums.JobStatus.Running ? DateTime.UtcNow : null;
            DateTime? endTime = (request.Status == KubeJob.Core.Enums.JobStatus.Succeeded || request.Status == KubeJob.Core.Enums.JobStatus.Failed) ? DateTime.UtcNow : null;

            await _repository.MarkRunStatusAsync(request.RunId, request.Status, request.ResultMsg, startTime, endTime, request.WorkerId, request.RowVersion);

            // Handle Retry Logic if failed
            if (request.Status == KubeJob.Core.Enums.JobStatus.Failed)
            {
                var failedRun = await _repository.GetJobRunAsync(request.RunId);
                if (failedRun != null && failedRun.Status == KubeJob.Core.Enums.JobStatus.Failed)
                {
                    var spec = await _repository.GetSpecAsync(failedRun.SpecId);
                    if (spec != null && spec.MaxRetries > 0)
                    {
                        var failedCount = await _repository.GetFailedRunsCountAsync(failedRun.BatchId, failedRun.ShardIndex);
                        if (failedCount <= spec.MaxRetries)
                        {
                            var newRunId = Guid.NewGuid().ToString().Substring(0, 16); // or deterministic
                            var retryRun = new JobRun
                            {
                                Id = $"{failedRun.BatchId}_{failedRun.ShardIndex}_retry{failedCount}",
                                SpecId = failedRun.SpecId,
                                BatchId = failedRun.BatchId,
                                ShardIndex = failedRun.ShardIndex,
                                Status = KubeJob.Core.Enums.JobStatus.Pending,
                                CreatedAt = DateTime.UtcNow
                            };
                            await _repository.InsertJobRunAsync(retryRun);
                        }
                    }
                }
            }

            return Ok();
        }
    }
}
