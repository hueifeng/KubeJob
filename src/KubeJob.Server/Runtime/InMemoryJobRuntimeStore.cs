using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Development and test reference state machine for the V2 runtime.
/// </summary>
public sealed partial class InMemoryJobRuntimeStore :
    IJobSubmissionStore,
    IWorkerSessionStore,
    IJobClaimStore,
    IJobCompletionStore,
    IJobQueryStore,
    IJobScheduleStore,
    IOutboxStore,
    IJobRuntimeDashboardStore,
    IJobRuntimeMaintenanceStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, JobRunRecord> _runs = new(StringComparer.Ordinal);
    private readonly Dictionary<string, JobAttemptRecord> _attempts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<string>> _attemptIdsByRun = new(StringComparer.Ordinal);
    private readonly Dictionary<string, WorkerSessionRecord> _sessions = new(StringComparer.Ordinal);
    private readonly Dictionary<string, JobScheduleRecord> _schedules = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _idempotency = new(StringComparer.Ordinal);
    private readonly Dictionary<string, OutboxMessageRecord> _outbox = new(StringComparer.Ordinal);

    private bool TryGetSession(
        string workerId,
        string sessionId,
        long sessionEpoch,
        out WorkerSessionRecord session) =>
        _sessions.TryGetValue(SessionKey(workerId, sessionId), out session!)
        && session.Epoch == sessionEpoch
        && session.State is WorkerSessionState.Ready or WorkerSessionState.Draining;

    private bool HasConcurrencyConflict(JobRunRecord candidate)
    {
        if (string.IsNullOrWhiteSpace(candidate.ConcurrencyKey))
        {
            return false;
        }

        return _runs.Values.Any(other =>
            !string.Equals(other.Id, candidate.Id, StringComparison.Ordinal)
            && other.Phase == JobPhase.Running
            && string.Equals(other.ConcurrencyKey, candidate.ConcurrencyKey, StringComparison.Ordinal));
    }

    private void AddWorkAvailableOutbox(JobRunRecord run, DateTimeOffset now)
    {
        var message = new OutboxMessageRecord
        {
            Id = NewId(),
            Queue = run.Queue,
            EventType = OutboxEventTypes.WorkAvailable,
            PayloadJson = JsonSerializer.Serialize(new { runId = run.Id, queue = run.Queue }),
            AvailableAt = run.AvailableAt > now ? run.AvailableAt : now,
            CreatedAt = now,
            State = OutboxDeliveryState.Pending
        };
        _outbox.Add(message.Id, message);
    }

    private static void Requeue(
        JobRunRecord run,
        DateTimeOffset availableAt,
        string? failureCode,
        string? failureMessage)
    {
        run.Phase = JobPhase.Pending;
        run.AvailableAt = availableAt;
        run.CurrentAttemptId = null;
        run.CurrentWorkerId = null;
        run.CurrentSessionId = null;
        run.FailureCode = failureCode;
        run.FailureMessage = failureMessage;
        run.Version++;
    }

    private static void MakeTerminal(
        JobRunRecord run,
        JobPhase phase,
        DateTimeOffset completedAt,
        string? failureCode,
        string? failureMessage)
    {
        run.Phase = phase;
        run.CompletedAt = completedAt;
        run.CurrentAttemptId = null;
        run.CurrentWorkerId = null;
        run.CurrentSessionId = null;
        run.FailureCode = failureCode;
        run.FailureMessage = failureMessage;
        run.Version++;
    }

    private static JobAttemptPhase MapAttemptPhase(JobAttemptOutcome outcome) => outcome switch
    {
        JobAttemptOutcome.Succeeded => JobAttemptPhase.Succeeded,
        JobAttemptOutcome.RetryableFailure => JobAttemptPhase.RetryableFailure,
        JobAttemptOutcome.PermanentFailure => JobAttemptPhase.PermanentFailure,
        JobAttemptOutcome.Canceled => JobAttemptPhase.Canceled,
        JobAttemptOutcome.TimedOut => JobAttemptPhase.TimedOut,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, null)
    };

    private static bool IsTerminal(JobPhase phase) => phase is
        JobPhase.Succeeded or JobPhase.Failed or JobPhase.Canceled or JobPhase.Dead;

    private static string SessionKey(string workerId, string sessionId) => $"{workerId}\n{sessionId}";

    private static string NewId() => Guid.NewGuid().ToString("N");
}
