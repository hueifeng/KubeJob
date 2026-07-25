using System;
using System.Threading.Tasks;
using FluentAssertions;
using KubeJob.Core.Domain;
using KubeJob.Core.Enums;
using KubeJob.Server.Data;

namespace KubeJob.Tests.Server.Data
{
    public class InMemoryKubeJobRepositoryTests
    {
        [Fact]
        public async Task AssignRunAsync_ShouldMoveRunToAssignedState()
        {
            var repo = new InMemoryKubeJobRepository();
            var run = new JobRun
            {
                Id = "run-1",
                SpecId = "spec-1",
                BatchId = "batch-1",
                Status = JobStatus.Pending,
                CreatedAt = DateTime.UtcNow,
                RowVersion = Guid.NewGuid().ToString()
            };
            await repo.InsertJobRunAsync(run);

            var assigned = await repo.AssignRunAsync(run.Id, "node-1", run.RowVersion!);

            assigned.Should().BeTrue();
            var updated = await repo.GetJobRunAsync(run.Id);
            updated.Should().NotBeNull();
            updated!.Status.Should().Be(JobStatus.Assigned);
            updated.TargetNodeId.Should().Be("node-1");
        }

        [Fact]
        public async Task CancelActiveRunsForSpecAsync_ShouldMarkRunsCanceled()
        {
            var repo = new InMemoryKubeJobRepository();
            await repo.InsertJobRunAsync(new JobRun
            {
                Id = "run-1",
                SpecId = "spec-1",
                BatchId = "batch-1",
                Status = JobStatus.Pending,
                CreatedAt = DateTime.UtcNow,
                RowVersion = Guid.NewGuid().ToString()
            });

            await repo.CancelActiveRunsForSpecAsync("spec-1", "replace");

            var updated = await repo.GetJobRunAsync("run-1");
            updated.Should().NotBeNull();
            updated!.Status.Should().Be(JobStatus.Canceled);
            updated.ResultMsg.Should().Be("replace");
            updated.EndTime.Should().NotBeNull();
        }

        [Fact]
        public async Task GetAssignedRunsForNodeAsync_ShouldReturnOnlyAssignedRuns()
        {
            var repo = new InMemoryKubeJobRepository();
            var rowVersion = Guid.NewGuid().ToString();
            await repo.InsertJobRunAsync(new JobRun
            {
                Id = "run-assigned",
                SpecId = "spec-1",
                BatchId = "batch-1",
                Status = JobStatus.Pending,
                CreatedAt = DateTime.UtcNow,
                RowVersion = rowVersion
            });
            await repo.AssignRunAsync("run-assigned", "node-1", rowVersion);

            await repo.InsertJobRunAsync(new JobRun
            {
                Id = "run-running",
                SpecId = "spec-1",
                BatchId = "batch-1",
                Status = JobStatus.Running,
                TargetNodeId = "node-1",
                CreatedAt = DateTime.UtcNow
            });

            var assigned = await repo.GetAssignedRunsForNodeAsync("node-1");

            assigned.Should().ContainSingle(r => r.Id == "run-assigned");
            assigned.Should().NotContain(r => r.Id == "run-running");
        }
    }
}
