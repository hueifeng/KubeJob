using System;
using System.Threading.Tasks;
using FluentAssertions;
using KubeJob.Core.Domain;
using KubeJob.Core.Enums;
using KubeJob.Server.Data;
using Xunit;

namespace KubeJob.Tests.Server.Data
{
    public class InMemoryKubeJobRepositoryTests
    {
        [Fact]
        public async Task AssignedRun_ShouldBeVisibleToItsWorker_AndFencedByAssignmentToken()
        {
            var repository = new InMemoryKubeJobRepository();
            var run = new JobRun
            {
                Id = "run-1",
                SpecId = "spec-1",
                BatchId = "batch-1",
                RowVersion = "token-1",
                Status = JobStatus.Pending,
                CreatedAt = DateTime.UtcNow
            };

            (await repository.InsertJobRunAsync(run)).Should().BeTrue();
            (await repository.AssignRunAsync(run.Id, "worker-1", "token-1")).Should().BeTrue();
            (await repository.GetAssignedRunsForNodeAsync("worker-1", 1)).Should().ContainSingle();
            var assignmentToken = (await repository.GetJobRunAsync(run.Id))!.RowVersion;

            await repository.MarkRunStatusAsync(run.Id, JobStatus.Succeeded,
                workerId: "stale-worker", rowVersion: assignmentToken);
            (await repository.GetJobRunAsync(run.Id))!.Status.Should().Be(JobStatus.Assigned);

            await repository.MarkRunStatusAsync(run.Id, JobStatus.Succeeded,
                workerId: "worker-1", rowVersion: assignmentToken);
            (await repository.GetJobRunAsync(run.Id))!.Status.Should().Be(JobStatus.Succeeded);
        }

        [Fact]
        public async Task CancelActiveRuns_ShouldUseCanceledStatus_AndInvalidateOldToken()
        {
            var repository = new InMemoryKubeJobRepository();
            var run = new JobRun
            {
                Id = "run-2",
                SpecId = "spec-2",
                BatchId = "batch-2",
                RowVersion = "token-2",
                Status = JobStatus.Pending,
                CreatedAt = DateTime.UtcNow
            };

            await repository.InsertJobRunAsync(run);
            await repository.CancelActiveRunsForSpecAsync(run.SpecId, "replaced");

            var canceled = await repository.GetJobRunAsync(run.Id);
            canceled!.Status.Should().Be(JobStatus.Canceled);
            canceled.RowVersion.Should().NotBe("token-2");
        }

        [Fact]
        public async Task PendingAndAssignedQueries_ShouldRespectLimits()
        {
            var repository = new InMemoryKubeJobRepository();
            for (var i = 0; i < 3; i++)
            {
                await repository.InsertJobRunAsync(new JobRun
                {
                    Id = $"run-{i}",
                    SpecId = "spec-3",
                    BatchId = "batch-3",
                    RowVersion = $"token-{i}",
                    Status = JobStatus.Pending,
                    CreatedAt = DateTime.UtcNow.AddSeconds(i)
                });
            }

            (await repository.GetPendingRunsAsync(2)).Should().HaveCount(2);
        }
    }
}
