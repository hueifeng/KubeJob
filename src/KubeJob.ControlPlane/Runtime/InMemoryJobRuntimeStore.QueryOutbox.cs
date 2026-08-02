using KubeJob.Core.Runtime;

namespace KubeJob.ControlPlane.Runtime;

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

    public ValueTask<IReadOnlyList<JobRunRecord>> GetRunsAsync(
        IReadOnlyList<string> runIds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(runIds);
        cancellationToken.ThrowIfCancellationRequested();
        if (runIds.Count == 0)
        {
            return ValueTask.FromResult<IReadOnlyList<JobRunRecord>>(Array.Empty<JobRunRecord>());
        }

        lock (_gate)
        {
            var ids = runIds.ToHashSet(StringComparer.Ordinal);
            return ValueTask.FromResult<IReadOnlyList<JobRunRecord>>(
                _runs.Values.Where(run => ids.Contains(run.Id)).ToArray());
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

        if (batchSize <= 0)
        {
            return new OutboxDispatchBatch(dispatched, failed, abandoned);
        }

        // Claim the whole batch up front in one locked pass, mirroring the
        // Postgres store's single bulk-claim transaction, instead of
        // re-acquiring the gate for every message.
        List<OutboxMessageRecord> claimedBatch;
        lock (_gate)
        {
            var now = DateTimeOffset.UtcNow;
            claimedBatch = _outbox.Values
                .Where(message => message.State is
                    OutboxDeliveryState.Pending or
                    OutboxDeliveryState.Failed or
                    OutboxDeliveryState.Publishing)
                .Where(message => message.AvailableAt <= now)
                .OrderBy(message => message.AvailableAt)
                .ThenBy(message => message.CreatedAt)
                .Take(batchSize)
                .ToList();

            foreach (var message in claimedBatch)
            {
                message.State = OutboxDeliveryState.Publishing;
                message.AvailableAt = now.Add(claimDuration);
                message.PublishAttempts++;
                message.ClaimToken = Guid.NewGuid().ToString("N");
            }
        }

        var pendingPublish = new List<OutboxPublication>(claimedBatch.Count);

        // Dispatch the whole claimed batch concurrently so broker confirms
        // pipeline (per-slot locks are held only for BasicPublish, not the
        // confirm wait). Per-message outcomes are inspected after Task.WhenAll;
        // the in-memory mark phase takes the lock one row at a time.
        var dispatchTasks = claimedBatch
            .Select(claimed => DispatchToTaskAsync(claimed, dispatch, cancellationToken))
            .ToArray();

        try
        {
            await Task.WhenAll(dispatchTasks).ConfigureAwait(false);
        }
        catch
        {
            // Task.WhenAll rethrows the first failure, but every task is now
            // complete; classify each outcome below.
        }

        for (var index = 0; index < claimedBatch.Count; index++)
        {
            var claimed = claimedBatch[index];
            var task = dispatchTasks[index];

            if (task.IsCompletedSuccessfully)
            {
                pendingPublish.Add(new OutboxPublication(claimed.Id, claimed.ClaimToken!));
                dispatched.Add(claimed.Id);
                continue;
            }

            var exception = task.IsFaulted
                ? task.Exception!.GetBaseException()
                : task.IsCanceled
                    ? new OperationCanceledException(cancellationToken)
                    : null;

            if (exception is PermanentOutboxException permanent)
            {
                await MarkAbandonedAsync(
                    new OutboxFailure(
                        claimed.Id,
                        claimed.ClaimToken!,
                        permanent.Message,
                        DateTimeOffset.UtcNow),
                    CancellationToken.None);
                abandoned.Add(claimed.Id);
            }
            else
            {
                var error = exception is OperationCanceledException
                    ? "publisher_canceled"
                    : exception?.Message ?? "publisher_canceled";
                lock (_gate)
                {
                    if (_outbox.TryGetValue(claimed.Id, out var message)
                        && message.State == OutboxDeliveryState.Publishing
                        && message.ClaimToken == claimed.ClaimToken)
                    {
                        message.State = OutboxDeliveryState.Failed;
                        message.LastError = error;
                        message.AvailableAt = DateTimeOffset.UtcNow.Add(retryDelay);
                        message.ClaimToken = null;
                    }
                }

                failed.Add(claimed.Id);
            }
        }

        FlushPublished(pendingPublish);

        if (cancellationToken.IsCancellationRequested)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }

        return new OutboxDispatchBatch(dispatched, failed, abandoned);
    }

    /// <summary>
    /// Invokes the per-message dispatch callback inside an async entry point so a
    /// synchronously thrown exception (e.g. <see cref="PermanentOutboxException"/>
    /// from an unknown event type) is captured into the returned <see cref="Task"/>
    /// instead of escaping the <see cref="Task.WhenAll(Task[])"/> setup. The
    /// caller inspects each task's outcome to classify it as published, failed, or
    /// abandoned.
    /// </summary>
    private static async Task DispatchToTaskAsync(
        OutboxMessageRecord claimed,
        Func<OutboxMessageRecord, CancellationToken, ValueTask> dispatch,
        CancellationToken cancellationToken)
    {
        await dispatch(claimed, cancellationToken).ConfigureAwait(false);
    }

    private void FlushPublished(List<OutboxPublication> pendingPublish)
    {
        if (pendingPublish.Count == 0)
        {
            return;
        }

        var publishedAt = DateTimeOffset.UtcNow;
        lock (_gate)
        {
            foreach (var publication in pendingPublish)
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

        pendingPublish.Clear();
    }
}
