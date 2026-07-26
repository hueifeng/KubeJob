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
                message.ClaimToken = Guid.NewGuid().ToString("N");
            }

            return ValueTask.FromResult<IReadOnlyList<OutboxMessageRecord>>(messages);
        }
    }

    public ValueTask MarkPublishedAsync(
        IReadOnlyList<OutboxPublication> publications,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            foreach (var publication in publications)
            {
                if (_outbox.TryGetValue(publication.MessageId, out var message)
                    && message.State == OutboxDeliveryState.Publishing
                    && message.ClaimToken == publication.ClaimToken)
                {
                    message.State = OutboxDeliveryState.Published;
                    message.PublishedAt = publishedAt;
                    message.LastError = null;
                    message.ClaimToken = null;
                }
            }
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask MarkFailedAsync(
        OutboxFailure failure,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_outbox.TryGetValue(failure.MessageId, out var message)
                && message.State == OutboxDeliveryState.Publishing
                && message.ClaimToken == failure.ClaimToken)
            {
                message.State = OutboxDeliveryState.Failed;
                message.LastError = failure.Error;
                message.AvailableAt = failure.NextAttemptAt;
                message.ClaimToken = null;
            }
        }

        return ValueTask.CompletedTask;
    }
}
