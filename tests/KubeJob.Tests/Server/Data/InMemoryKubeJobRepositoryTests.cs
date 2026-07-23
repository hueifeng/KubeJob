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
        private readonly InMemoryKubeJobRepository _repository = new();

        [Fact]
        public async Task AssignRunAsync_ShouldMovePendingRunToAssigned()
        {
            var run = NewRun();
            await _repository.InsertJobRunAsync(run);

            var assigned = await _repository.AssignRunAsync(run.Id, "worker-1", run.RowVersion!);

            assigned.Should().BeTrue();
            var stored = await _repository.GetJobRunAsync(run.Id);
            stored!.Status.Should().Be(JobStatus.Assigned);
            stored.TargetNodeId.Should().Be("worker-1");
        }

        [Fact]
        public async Task TryTransitionRunStatusAsync_ShouldRejectWrongWorkerAndStaleStatus()
        {
            var run = NewRun();
            await _repository.InsertJobRunAsync(run);
            await _repository.AssignRunAsync(run.Id, "worker-1", run.RowVersion!);

            var wrongWorker = await _repository.TryTransitionRunStatusAsync(
                run.Id,
                "worker-2",
                JobStatus.Assigned,
                JobStatus.Running);
            var started = await _repository.TryTransitionRunStatusAsync(
                run.Id,
                "worker-1",
                JobStatus.Assigned,
                JobStatus.Running,
                startTime: DateTime.UtcNow);
            var staleStart = await _repository.TryTransitionRunStatusAsync(
                run.Id,
                "worker-1",
                JobStatus.Assigned,
                JobStatus.Running);

            wrongWorker.Should().BeFalse();
            started.Should().BeTrue();
            staleStart.Should().BeFalse();
        }

        [Fact]
        public async Task CancelActiveRunsForSpecAsync_ShouldUseCanceledState()
        {
            var run = NewRun();
            await _repository.InsertJobRunAsync(run);

            await _repository.CancelActiveRunsForSpecAsync(run.SpecId, "replaced");

            var stored = await _repository.GetJobRunAsync(run.Id);
            stored!.Status.Should().Be(JobStatus.Canceled);
            stored.ResultMsg.Should().Be("replaced");
            stored.EndTime.Should().NotBeNull();
        }

        [Fact]
        public async Task DeleteOldRunsAsync_ShouldNotDeleteActiveRuns()
        {
            var active = NewRun(createdAt: DateTime.UtcNow.AddDays(-30));
            var terminal = NewRun(id: "terminal", createdAt: DateTime.UtcNow.AddDays(-30));
            terminal.Status = JobStatus.Succeeded;

            await _repository.InsertJobRunAsync(active);
            await _repository.InsertJobRunAsync(terminal);

            var deleted = await _repository.DeleteOldRunsAsync(DateTime.UtcNow.AddDays(-7));

            deleted.Should().Be(1);
            (await _repository.GetJobRunAsync(active.Id)).Should().NotBeNull();
            (await _repository.GetJobRunAsync(terminal.Id)).Should().BeNull();
        }

        [Fact]
        public async Task Heartbeat_ShouldBringOfflineNodeBackOnline()
        {
            var node = new WorkerNode
            {
                Id = "worker-1",
                LastHeartbeat = DateTime.UtcNow.AddMinutes(-10),
                MaxCapacity = 2,
                IsOffline = false
            };
            await _repository.UpsertWorkerNodeAsync(node);
            await _repository.MarkNodesOfflineAsync(DateTime.UtcNow.AddMinutes(-1));

            await _repository.UpsertWorkerNodeAsync(new WorkerNode
            {
                Id = "worker-1",
                LastHeartbeat = DateTime.UtcNow,
                MaxCapacity = 2,
                IsOffline = true
            });

            var stored = (await _repository.GetAllNodesAsync()).Should().ContainSingle().Subject;
            stored.IsOffline.Should().BeFalse();
        }

        private static JobRun NewRun(
            string id = "run-1",
            DateTime? createdAt = null)
        {
            return new JobRun
            {
                Id = id,
                SpecId = "spec-1",
                BatchId = "batch-1",
                Status = JobStatus.Pending,
                CreatedAt = createdAt ?? DateTime.UtcNow,
                RowVersion = Guid.NewGuid().ToString()
            };
        }
    }
}
