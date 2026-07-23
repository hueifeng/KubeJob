using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<SubmitJobResult> SubmitAsync(
        SubmitJobCommand command,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!string.IsNullOrWhiteSpace(command.IdempotencyKey)
                && _idempotency.TryGetValue(command.IdempotencyKey, out var existingId)
                && _runs.TryGetValue(existingId, out var existing))
            {
                return ValueTask.FromResult(new SubmitJobResult(existing, Existing: true));
            }

            var now = DateTimeOffset.UtcNow;
            var run = new JobRunRecord
            {
                Id = NewId(),
                JobKey = command.JobKey,
                PayloadJson = command.PayloadJson,
                Queue = command.Queue,
                Priority = command.Priority,
                AvailableAt = command.AvailableAt,
                CreatedAt = now,
                IdempotencyKey = command.IdempotencyKey,
                ConcurrencyKey = command.ConcurrencyKey,
                MaxAttempts = command.MaxAttempts,
                TimeoutSeconds = command.TimeoutSeconds,
                Phase = JobPhase.Pending
            };

            _runs.Add(run.Id, run);
            if (!string.IsNullOrWhiteSpace(command.IdempotencyKey))
            {
                _idempotency.Add(command.IdempotencyKey, run.Id);
            }

            AddWorkAvailableOutbox(run, now);
            return ValueTask.FromResult(new SubmitJobResult(run, Existing: false));
        }
    }

    public ValueTask<bool> RequestCancelAsync(
        string runId,
        string? reason,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_runs.TryGetValue(runId, out var run) || IsTerminal(run.Phase))
            {
                return ValueTask.FromResult(false);
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

            return ValueTask.FromResult(true);
        }
    }
}
