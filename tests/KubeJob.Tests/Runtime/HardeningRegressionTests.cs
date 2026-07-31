using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Locks in the behavior of the H/M-tier fixes that don't already have
/// dedicated coverage:
///   H4  AdmitAsync's worker_not_capable fallback returns Retry so the broker
///       rebalances the envelope instead of dropping it.
///   M4  CompletionBatcher restarts its loop and a fresh channel after the
///       previous loop has exited, so a transient store failure doesn't strand
///       later callers.
///   M6  The default no-op notifier and cancel publisher are wired by default
///       so Pull deployments don't depend on a transport package being loaded.
/// </summary>
public sealed class HardeningRegressionTests
{
    [Fact]
    public async Task Admission_returns_retry_when_worker_lacks_the_required_capability()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        await using var provider = services.BuildServiceProvider();
        var jobs = provider.GetRequiredService<JobControlPlane>();
        var workers = provider.GetRequiredService<WorkerControlPlane>();

        var receipt = await jobs.SubmitAsync(
            new EnqueueJobRequest(
                "order-push-3",
                "{\"orderId\":\"O-2001\"}",
                "orders.push",
                IdempotencyKey: "order-event:2001"));

        // The session advertises both queues/capabilities (broad), so the
        // targeted ClaimAsync filter doesn't drop the row on its own. The
        // envelope's request then reports a narrower capability set, which
        // is the broker mis-routing signal we want the fallback path to
        // detect.
        var session = await workers.RegisterAsync(
            new RegisterWorkerSessionRequest(
                "worker-broad",
                "session-broad",
                "test",
                "localhost",
                1,
                new[] { "orders.push", "reports.generate" },
                new[] { "order-push-3", "report.generate" },
                new Dictionary<string, string>()));

        var admission = await workers.AdmitAsync(new AdmitExecutionRequest(
            session.WorkerId,
            session.SessionId,
            session.SessionEpoch,
            1,
            receipt.Handle.JobId,
            // request queues/capabilities do NOT cover order-push-3,
            // simulating the broker handing us an envelope we can't run.
            new[] { "reports.generate" },
            new[] { "report.generate" }));

        admission.Status.Should().Be(ExecutionAdmissionStatus.Retry);
        admission.Reason.Should().Be("worker_not_capable");
    }

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
    public void Default_di_wires_noop_notifier_and_cancel_publisher()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IWorkAvailableNotifier>()
            .Should().BeOfType<NoopWorkAvailableNotifier>();
        provider.GetRequiredService<ICancelPublisher>()
            .Should().BeOfType<NoopCancelPublisher>();
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
