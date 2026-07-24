using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;

namespace KubeJob.Tests.Runtime;

public sealed class JobClientWaitTests
{
    [Fact]
    public async Task Wait_returns_the_first_terminal_snapshot()
    {
        var client = new SequenceJobClient(
            Snapshot(JobPhase.Pending),
            Snapshot(JobPhase.Running),
            Snapshot(JobPhase.Succeeded));

        var result = await client.WaitForCompletionAsync(
            "run-1",
            TimeSpan.FromMilliseconds(50));

        result.Phase.Should().Be(JobPhase.Succeeded);
        client.ReadCount.Should().Be(3);
    }

    [Fact]
    public async Task Wait_honors_cancellation()
    {
        var client = new SequenceJobClient(Snapshot(JobPhase.Running));
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(80));

        var action = async () => await client.WaitForCompletionAsync(
            "run-1",
            TimeSpan.FromMilliseconds(50),
            cancellation.Token);

        await action.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Wait_rejects_an_unbounded_poll_rate()
    {
        var client = new SequenceJobClient(Snapshot(JobPhase.Running));

        var action = async () => await client.WaitForCompletionAsync(
            "run-1",
            TimeSpan.Zero);

        await action.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    private static JobStatusSnapshot Snapshot(JobPhase phase) => new(
        "run-1",
        phase,
        1,
        DateTimeOffset.UtcNow,
        null,
        null,
        null,
        null,
        null);

    private sealed class SequenceJobClient : IJobClient
    {
        private readonly Queue<JobStatusSnapshot> _snapshots;
        private JobStatusSnapshot? _last;

        public SequenceJobClient(params JobStatusSnapshot[] snapshots)
        {
            _snapshots = new Queue<JobStatusSnapshot>(snapshots);
        }

        public int ReadCount { get; private set; }

        public ValueTask<JobHandle> EnqueueAsync<TPayload>(
            JobKey<TPayload> job,
            TPayload payload,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<JobHandle> EnqueueAsync<TPayload>(
            JobKey<TPayload> job,
            TPayload payload,
            JobEnqueueOptions options,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<JobStatusSnapshot?> GetStatusAsync(
            string jobId,
            CancellationToken cancellationToken = default)
        {
            ReadCount++;
            if (_snapshots.Count > 0)
            {
                _last = _snapshots.Dequeue();
            }
            return ValueTask.FromResult(_last);
        }

        public ValueTask<bool> CancelAsync(
            string jobId,
            string? reason = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
