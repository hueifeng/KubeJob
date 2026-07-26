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

    public ValueTask MarkAbandonedAsync(
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
                message.State = OutboxDeliveryState.Abandoned;
                message.LastError = failure.Error;
                message.ClaimToken = null;
            }
        }

        return ValueTask.CompletedTask;
    }

    public async ValueTask<OutboxDispatchBatch> DispatchOnceAsync(
        TimeSpan claimDuration,
        TimeSpan retryDelay,
        int batchSize,
        Func<OutboxMessageRecord, CancellationToken, ValueTask> dispatch,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dispatch);

        if (claimDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(claimDuration));
        }

        if (retryDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retryDelay));
        }

        var dispatched = new List<string>(Math.Max(0, batchSize));
        var failed = new List<string>(Math.Max(0, batchSize));
        var abandoned = new List<string>(Math.Max(0, batchSize));
        var processed = 0;

        if (batchSize <= 0)
        {
            return new OutboxDispatchBatch(dispatched, failed, abandoned);
        }

        while (processed < batchSize && !cancellationToken.IsCancellationRequested)
        {
            OutboxMessageRecord? claimed;
            lock (_gate)
            {
                var now = DateTimeOffset.UtcNow;
                var candidate = _outbox.Values
                    .Where(message => message.State is
                        OutboxDeliveryState.Pending or
                        OutboxDeliveryState.Failed or
                        OutboxDeliveryState.Publishing)
                    .Where(message => message.AvailableAt <= now)
                    .OrderBy(message => message.AvailableAt)
                    .ThenBy(message => message.CreatedAt)
                    .FirstOrDefault();
                if (candidate is null)
                {
                    break;
                }

                candidate.State = OutboxDeliveryState.Publishing;
                candidate.AvailableAt = now.Add(claimDuration);
                candidate.PublishAttempts++;
                candidate.ClaimToken = Guid.NewGuid().ToString("N");
                claimed = candidate;
            }

            try
            {
                await dispatch(claimed, cancellationToken).ConfigureAwait(false);

                lock (_gate)
                {
                    if (_outbox.TryGetValue(claimed.Id, out var message)
                        && message.State == OutboxDeliveryState.Publishing
                        && message.ClaimToken == claimed.ClaimToken)
                    {
                        message.State = OutboxDeliveryState.Published;
                        message.PublishedAt = DateTimeOffset.UtcNow;
                        message.LastError = null;
                        message.ClaimToken = null;
                    }
                }

                dispatched.Add(claimed.Id);
                processed++;
            }
            catch (PermanentOutboxException ex)
            {
                await MarkAbandonedAsync(
                    new OutboxFailure(
                        claimed.Id,
                        claimed.ClaimToken!,
                        ex.Message,
                        DateTimeOffset.UtcNow),
                    CancellationToken.None);
                abandoned.Add(claimed.Id);
                processed++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Worker is shutting down. Roll the claim back so a different
                // publisher (or the next iteration) can pick it up.
                lock (_gate)
                {
                    if (_outbox.TryGetValue(claimed.Id, out var message)
                        && message.State == OutboxDeliveryState.Publishing
                        && message.ClaimToken == claimed.ClaimToken)
                    {
                        message.State = OutboxDeliveryState.Failed;
                        message.LastError = "publisher_canceled";
                        message.AvailableAt = DateTimeOffset.UtcNow.Add(retryDelay);
                        message.ClaimToken = null;
                    }
                }

                failed.Add(claimed.Id);
                throw;
            }
            catch (Exception ex)
            {
                lock (_gate)
                {
                    if (_outbox.TryGetValue(claimed.Id, out var message)
                        && message.State == OutboxDeliveryState.Publishing
                        && message.ClaimToken == claimed.ClaimToken)
                    {
                        message.State = OutboxDeliveryState.Failed;
                        message.LastError = ex.Message;
                        message.AvailableAt = DateTimeOffset.UtcNow.Add(retryDelay);
                        message.ClaimToken = null;
                    }
                }

                failed.Add(claimed.Id);
                processed++;
            }
        }

        return new OutboxDispatchBatch(dispatched, failed, abandoned);
    }
}
