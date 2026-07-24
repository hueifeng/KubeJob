using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<JobRunRecord?> GetRunAsync(
        string runId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            _runs.TryGetValue(runId, out var run);
            return ValueTask.FromResult(run);
        }
    }

    public ValueTask<IReadOnlyList<JobAttemptRecord>> GetAttemptsAsync(
        string runId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_attemptIdsByRun.TryGetValue(runId, out var ids))
            {
                return ValueTask.FromResult<IReadOnlyList<JobAttemptRecord>>(Array.Empty<JobAttemptRecord>());
            }

            return ValueTask.FromResult<IReadOnlyList<JobAttemptRecord>>(
                ids.Select(id => _attempts[id]).ToArray());
        }
    }

    public ValueTask<IReadOnlyList<OutboxMessageRecord>> ClaimPendingAsync(
        DateTimeOffset now,
        TimeSpan claimDuration,
        int batchSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (claimDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(claimDuration));
        }

        lock (_gate)
        {
            var messages = _outbox.Values
                .Where(message => message.State is
                    OutboxDeliveryState.Pending or
                    OutboxDeliveryState.Failed or
                    OutboxDeliveryState.Publishing)
                .Where(message => message.AvailableAt <= now)
                .OrderBy(message => message.AvailableAt)
                .ThenBy(message => message.CreatedAt)
                .Take(Math.Max(0, batchSize))
                .ToArray();

            foreach (var message in messages)
            {
                message.State = OutboxDeliveryState.Publishing;
                message.AvailableAt = now.Add(claimDuration);
                message.PublishAttempts++;
            }

            return ValueTask.FromResult<IReadOnlyList<OutboxMessageRecord>>(messages);
        }
    }

    public ValueTask MarkPublishedAsync(
        string messageId,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_outbox.TryGetValue(messageId, out var message))
            {
                message.State = OutboxDeliveryState.Published;
                message.PublishedAt = publishedAt;
                message.LastError = null;
            }
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask MarkFailedAsync(
        string messageId,
        string error,
        DateTimeOffset nextAttemptAt,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_outbox.TryGetValue(messageId, out var message))
            {
                message.State = OutboxDeliveryState.Failed;
                message.LastError = error;
                message.AvailableAt = nextAttemptAt;
            }
        }

        return ValueTask.CompletedTask;
    }
}
