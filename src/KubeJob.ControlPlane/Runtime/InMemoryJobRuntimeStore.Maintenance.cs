using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.ControlPlane.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<int> DeletePublishedOutboxAsync(
        DateTimeOffset olderThan,
        int batchSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (batchSize <= 0)
        {
            return ValueTask.FromResult(0);
        }

        lock (_gate)
        {
            var ids = _outbox.Values
                .Where(message => message.State == OutboxDeliveryState.Published)
                .Where(message => message.PublishedAt is not null && message.PublishedAt <= olderThan)
                .OrderBy(message => message.PublishedAt)
                .ThenBy(message => message.Id)
                .Take(batchSize)
                .Select(message => message.Id)
                .ToArray();
            foreach (var id in ids)
            {
                _outbox.Remove(id);
            }

            return ValueTask.FromResult(ids.Length);
        }
    }

    public ValueTask<int> DeleteUnkeyedTerminalRunsAsync(
        DateTimeOffset olderThan,
        int batchSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (batchSize <= 0)
        {
            return ValueTask.FromResult(0);
        }

        lock (_gate)
        {
            var runs = _runs.Values
                .Where(run => run.Phase is JobPhase.Succeeded or JobPhase.Failed or JobPhase.Canceled or JobPhase.Dead)
                .Where(run => run.CompletedAt is not null && run.CompletedAt <= olderThan)
                .Where(run => run.IdempotencyKey is null && run.ScheduleId is null)
                .OrderBy(run => run.CompletedAt)
                .ThenBy(run => run.Id)
                .Take(batchSize)
                .ToArray();

            foreach (var run in runs)
            {
                if (_attemptIdsByRun.TryGetValue(run.Id, out var attemptIds))
                {
                    foreach (var attemptId in attemptIds)
                    {
                        _attempts.Remove(attemptId);
                    }

                    _attemptIdsByRun.Remove(run.Id);
                }

                _runs.Remove(run.Id);
            }

            return ValueTask.FromResult(runs.Length);
        }
    }
}
