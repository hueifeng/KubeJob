using System;
using System.Text.Json;
using System.Threading.Tasks;
using KubeJob.Core.Domain;
using KubeJob.Core.Enums;
using KubeJob.Server.Data;
using Microsoft.AspNetCore.Mvc;

namespace KubeJob.Server.Controllers
{
    [ApiController]
    [Route("api/seed")]
    public class SeedController : ControllerBase
    {
        private readonly IKubeJobRepository _repository;

        public SeedController(IKubeJobRepository repository)
        {
            _repository = repository;
        }

        [HttpPost]
        public async Task<IActionResult> Seed()
        {
            for (int i = 1; i <= 50; i++)
            {
                var policy = i % 3 == 0 ? ConcurrencyPolicy.Replace :
                             i % 2 == 0 ? ConcurrencyPolicy.Forbid : ConcurrencyPolicy.Allow;
                             
                var spec = new JobSpec
                {
                    Id = $"job-batch-{i}",
                    Name = $"Diverse Job {i}",
                    JobType = "SampleDataJob",
                    CronExpression = i % 5 == 0 ? $"*/{i%4 + 2} * * * *" : // e.g., */2, */3, */4, */5
                                     i % 7 == 0 ? $"{i%60} * * * *" :      // specific minute
                                     "*/1 * * * *",                        // default every minute
                    NodeSelector = "{}",
                    ExecuteModel = ExecuteModel.Standalone,
                    TotalShards = 1,
                    IsDisabled = false,
                    TimeoutSeconds = 30 + (i * 10), // variable timeouts
                    MaxRetries = i % 3,
                    ConcurrencyPolicy = policy,
                    NextRunTime = DateTime.UtcNow.AddSeconds(i * 5)
                };

                await _repository.UpsertSpecAsync(spec);
            }
            
            return Ok("Seeded 50 diverse job specs.");
        }
    }
}
