using System.Text.Json;
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
                JobSubmissionIdentity.EnsureCompatible(existing, command);
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
                AvailableAt = command.AvailableAt.ToUniversalTime(),
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
        string? consumerGroup,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_runs.TryGetValue(runId, out var run))
            {
                return ValueTask.FromResult(new CancelJobResult(false, null, null));
            }

            if (IsTerminal(run.Phase) || run.CancelRequested)
            {
                return ValueTask.FromResult(new CancelJobResult(false, run.Queue, consumerGroup));
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

            if (!string.IsNullOrWhiteSpace(consumerGroup))
            {
                var message = new OutboxMessageRecord
                {
                    Id = NewId(),
                    Queue = consumerGroup!,
                    EventType = OutboxEventTypes.Cancel,
                    PayloadJson = JsonSerializer.Serialize(new { runId = run.Id }),
                    AvailableAt = DateTimeOffset.UtcNow,
                    CreatedAt = DateTimeOffset.UtcNow,
                    State = OutboxDeliveryState.Pending
                };
                _outbox.Add(message.Id, message);
            }

            return ValueTask.FromResult(new CancelJobResult(true, run.Queue, consumerGroup));
        }
    }
}
