using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.ControlPlane.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<SubmitJobResult> SubmitAsync(
        SubmitJobCommand command,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult(SubmitCore(command));
        }
    }

    public ValueTask<IReadOnlyList<SubmitJobResult>> SubmitBatchAsync(
        IReadOnlyList<SubmitJobCommand> commands,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(commands);
        cancellationToken.ThrowIfCancellationRequested();

        var results = new SubmitJobResult[commands.Count];
        lock (_gate)
        {
            ValidateBatchCommands(commands);
            for (var index = 0; index < commands.Count; index++)
            {
                results[index] = SubmitCore(commands[index]);
            }
        }

        return ValueTask.FromResult<IReadOnlyList<SubmitJobResult>>(results);
    }

    private void ValidateBatchCommands(IReadOnlyList<SubmitJobCommand> commands)
    {
        var newKeys = new Dictionary<string, SubmitJobCommand>(StringComparer.Ordinal);
        foreach (var command in commands)
        {
            var target = command.DeliveryTarget
                ?? new DeliveryTarget(ExecutionDeliveryProfile.Pull, "default", null, "default");
            target.Validate();

            if (string.IsNullOrWhiteSpace(command.IdempotencyKey))
            {
                continue;
            }

            if (_idempotency.TryGetValue(command.IdempotencyKey, out var existingId)
                && _runs.TryGetValue(existingId, out var existing))
            {
                JobSubmissionIdentity.EnsureCompatible(existing, command);
                continue;
            }

            if (newKeys.TryGetValue(command.IdempotencyKey, out var earlier))
            {
                JobSubmissionIdentity.EnsureCompatible(earlier, command);
            }
            else
            {
                newKeys.Add(command.IdempotencyKey, command);
            }
        }
    }

    private SubmitJobResult SubmitCore(SubmitJobCommand command)
    {
        if (!string.IsNullOrWhiteSpace(command.IdempotencyKey)
            && _idempotency.TryGetValue(command.IdempotencyKey, out var existingId)
            && _runs.TryGetValue(existingId, out var existing))
        {
            JobSubmissionIdentity.EnsureCompatible(existing, command);
            return new SubmitJobResult(existing, Existing: true);
        }

        var now = DateTimeOffset.UtcNow;
        var target = command.DeliveryTarget
            ?? new DeliveryTarget(ExecutionDeliveryProfile.Pull, "default", null, "default");
        target.Validate();
        var run = new JobRunRecord
        {
            Id = NewId(),
            JobKey = command.JobKey,
            PayloadJson = command.PayloadJson,
            Queue = command.Queue,
            DeliveryProfile = ExecutionDeliveryProfile.Pull,
            ExecutionLane = target.ExecutionLane,
            ConsumerGroup = target.ConsumerGroup,
            TransportId = null,
            Priority = command.Priority,
            AvailableAt = command.AvailableAt.ToUniversalTime(),
            CreatedAt = now,
            IdempotencyKey = command.IdempotencyKey,
            ConcurrencyKey = command.ConcurrencyKey,
            OrderingMode = target.OrderingMode,
            OrderingSequence = ++_nextOrderingSequence,
            MaxAttempts = command.MaxAttempts,
            TimeoutSeconds = command.TimeoutSeconds,
            RetryPolicy = command.RetryPolicy,
            Continuation = command.Continuation,
            Compensation = command.Compensation,
            Phase = JobPhase.Pending
        };

        _runs.Add(run.Id, run);
        if (!string.IsNullOrWhiteSpace(command.IdempotencyKey))
        {
            _idempotency.Add(command.IdempotencyKey, run.Id);
        }

        AddWorkAvailableOutbox(run, now);
        return new SubmitJobResult(run, Existing: false);
    }

    public ValueTask<bool> RequeueWorkAvailableAsync(
        string runId,
        DateTimeOffset availableAt,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_runs.TryGetValue(runId, out var run)
                || run.Phase != JobPhase.Pending
                || run.CancelRequested)
            {
                return ValueTask.FromResult(false);
            }

            var now = DateTimeOffset.UtcNow;
            run.AvailableAt = run.AvailableAt > availableAt
                ? run.AvailableAt
                : availableAt;
            run.Version++;
            AddWorkAvailableOutbox(run, now);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<CancelJobResult> RequestCancelAsync(
        string runId,
        string? reason,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_runs.TryGetValue(runId, out var run))
            {
                return ValueTask.FromResult(new CancelJobResult(false));
            }

            if (IsTerminal(run.Phase) || run.CancelRequested)
            {
                return ValueTask.FromResult(new CancelJobResult(false));
            }

            run.CancelRequested = true;
            run.FailureCode = "cancel_requested";
            run.FailureMessage = reason;
            run.Version++;

            if (run.Phase == JobPhase.Pending)
            {
                run.Phase = JobPhase.Canceled;
                run.CompletedAt = DateTimeOffset.UtcNow;
            }

            // PostgresManaged cancellation is database-authoritative. Running
            // workers observe cancel state through the normal managed runtime;
            // no transport control message or outbox row is produced.
            return ValueTask.FromResult(new CancelJobResult(true));
        }
    }

    public ValueTask<JobRunRecord?> GetByIdempotencyKeyAsync(
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            foreach (var run in _runs.Values)
            {
                if (string.Equals(run.IdempotencyKey, idempotencyKey, StringComparison.Ordinal)
                    && run.Phase != JobPhase.Canceled
                    && run.Phase != JobPhase.Failed
                    && run.Phase != JobPhase.Succeeded
                    && run.Phase != JobPhase.Dead)
                {
                    return ValueTask.FromResult<JobRunRecord?>(run);
                }
            }

            return ValueTask.FromResult<JobRunRecord?>(null);
        }
    }
}
