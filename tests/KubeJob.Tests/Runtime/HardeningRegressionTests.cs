using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Locks in the behavior of the H/M-tier fixes that don't already have
/// dedicated coverage:
///   M4  CompletionBatcher restarts its loop and a fresh channel after the
///       previous loop has exited, so a transient store failure doesn't strand
///       later callers.
///   M6  The default no-op notifier is wired by default so PostgresManaged
///       deployments don't depend on a transport package being loaded.
/// </summary>
public sealed class HardeningRegressionTests
{
    [Fact]
    public async Task Completion_batcher_restarts_after_the_loop_exits()
    {
        var store = new FlakyCompletionStore();
        var batcher = new CompletionBatcher(
            store,
            Options.Create(new JobRuntimeOptions
            {
                CompletionBatchSize = 4,
                CompletionFlushInterval = TimeSpan.FromMilliseconds(50)
            }));

        // First batch: store is healthy and completes successfully.
        var first = await batcher.EnqueueAsync(
            NewCompletion("run-1"),
            CancellationToken.None);
        first.Accepted.Should().BeTrue();

        // Second batch: the store throws on CompleteBatchAsync, which
        // surfaces per-caller instead of leaving the caller hanging.
        store.ThrowOnNextBatch = true;

        var action = async () => await batcher.EnqueueAsync(
            NewCompletion("run-2"),
            CancellationToken.None);
        await action.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*synthetic_failure*");

        // Third batch: store recovers, but the previous loop has exited. The
        // batcher must restart the loop so this call doesn't hang forever.
        store.ThrowOnNextBatch = false;

        var third = await batcher.EnqueueAsync(
            NewCompletion("run-3"),
            CancellationToken.None);
        third.Accepted.Should().BeTrue();
    }

    [Fact]
    public void Default_di_wires_noop_notifier()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IWorkAvailableNotifier>()
            .Should().BeOfType<NoopWorkAvailableNotifier>();
    }

    private static CompleteAttemptRequest NewCompletion(string runId) =>
        new(
            WorkerId: "worker-x",
            SessionId: "session-x",
            SessionEpoch: 1,
            RunId: runId,
            AttemptId: $"attempt-{runId}",
            AttemptNumber: 1,
            LeaseToken: Guid.NewGuid().ToString("N"),
            Outcome: JobAttemptOutcome.Succeeded,
            FailureCode: null,
            FailureMessage: null);

    private sealed class FlakyCompletionStore : IJobCompletionStore
    {
        public bool ThrowOnNextBatch { get; set; }

        public ValueTask<CompleteAttemptResponse> CompleteAsync(
            CompleteAttemptRequest request,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken)
        {
            if (ThrowOnNextBatch)
            {
                throw new InvalidOperationException("synthetic_failure");
            }

            return ValueTask.FromResult(new CompleteAttemptResponse(
                true, JobPhase.Succeeded, false));
        }

        public ValueTask<IReadOnlyList<CompleteAttemptResponse>> CompleteBatchAsync(
            IReadOnlyList<CompleteAttemptRequest> requests,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken)
        {
            if (ThrowOnNextBatch)
            {
                throw new InvalidOperationException(
                    $"synthetic_failure: response count 0 != request count {requests.Count}");
            }

            IReadOnlyList<CompleteAttemptResponse> responses = requests
                .Select(_ => new CompleteAttemptResponse(
                    true, JobPhase.Succeeded, false))
                .ToArray();
            return ValueTask.FromResult(responses);
        }

        public ValueTask<int> RequeueExpiredLeasesAsync(
            DateTimeOffset now,
            RetryPolicy retryPolicy,
            int batchSize,
            CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(0);
        }
    }
}
