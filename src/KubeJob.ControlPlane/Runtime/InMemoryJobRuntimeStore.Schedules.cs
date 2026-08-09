using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;

namespace KubeJob.ControlPlane.Runtime;

public sealed partial class InMemoryJobRuntimeStore
{
    public ValueTask<JobScheduleRecord?> CreateIfAbsentAsync(
        JobScheduleRecord schedule,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_schedules.ContainsKey(schedule.Id))
            {
                return ValueTask.FromResult<JobScheduleRecord?>(null);
            }

            var now = DateTimeOffset.UtcNow;
            var stored = CloneSchedule(schedule);
            stored.CreatedAt = now;
            stored.UpdatedAt = now;
            stored.Version = 1;
            stored.ClaimToken = null;
            stored.ClaimUntil = null;
            _schedules[schedule.Id] = stored;
            return ValueTask.FromResult<JobScheduleRecord?>(CloneSchedule(stored));
        }
    }

    public ValueTask<JobScheduleRecord> UpsertAsync(
        JobScheduleRecord schedule,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var now = DateTimeOffset.UtcNow;
            _schedules.TryGetValue(schedule.Id, out var existing);
            var stored = CloneSchedule(schedule);
            stored.CreatedAt = existing?.CreatedAt ?? now;
            stored.UpdatedAt = now;
            stored.Version = (existing?.Version ?? 0) + 1;
            stored.ClaimToken = null;
            stored.ClaimUntil = null;
            _schedules[schedule.Id] = stored;
            return ValueTask.FromResult(CloneSchedule(stored));
        }
    }

    public ValueTask<JobScheduleRecord?> GetAsync(
        string scheduleId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult(
                _schedules.TryGetValue(scheduleId, out var schedule)
                    ? CloneSchedule(schedule)
                    : null);
        }
    }

    public ValueTask<bool> SetEnabledAsync(
        string scheduleId,
        bool enabled,
        DateTimeOffset? nextFireAt,
        long? expectedVersion,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_schedules.TryGetValue(scheduleId, out var schedule)
                || (expectedVersion is not null && schedule.Version != expectedVersion))
            {
                return ValueTask.FromResult(false);
            }

            schedule.Enabled = enabled;
            if (nextFireAt is not null)
            {
                schedule.NextFireAt = nextFireAt.Value.ToUniversalTime();
            }
            schedule.ClaimToken = null;
            schedule.ClaimUntil = null;
            schedule.UpdatedAt = DateTimeOffset.UtcNow;
            schedule.Version++;
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> DeleteAsync(
        string scheduleId,
        long? expectedVersion,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_schedules.TryGetValue(scheduleId, out var schedule)
                || (expectedVersion is not null && schedule.Version != expectedVersion))
            {
                return ValueTask.FromResult(false);
            }

            return ValueTask.FromResult(_schedules.Remove(scheduleId));
        }
    }

    public ValueTask<IReadOnlyList<ClaimedSchedule>> ClaimDueAsync(
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
            var utcNow = now.ToUniversalTime();
            var due = _schedules.Values
                .Where(schedule => schedule.Enabled)
                .Where(schedule => schedule.NextFireAt <= utcNow)
                .Where(schedule => schedule.ClaimUntil is null || schedule.ClaimUntil <= utcNow)
                .OrderBy(schedule => schedule.NextFireAt)
                .ThenBy(schedule => schedule.Id, StringComparer.Ordinal)
                .Take(Math.Max(0, batchSize))
                .ToArray();

            var claims = new List<ClaimedSchedule>(due.Length);
            foreach (var schedule in due)
            {
                schedule.ClaimToken = NewId();
                schedule.ClaimUntil = utcNow.Add(claimDuration);
                schedule.Version++;
                claims.Add(new ClaimedSchedule(
                    CloneSchedule(schedule),
                    schedule.ClaimToken,
                    schedule.Version));
            }

            return ValueTask.FromResult<IReadOnlyList<ClaimedSchedule>>(claims);
        }
    }

    public ValueTask<JobRunRecord?> CommitFireAsync(
        CommitScheduleFireCommand command,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var now = DateTimeOffset.UtcNow;
            if (!_schedules.TryGetValue(command.ScheduleId, out var schedule)
                || schedule.Version != command.ExpectedVersion
                || schedule.ClaimUntil is null
                || schedule.ClaimUntil <= now
                || !string.Equals(schedule.ClaimToken, command.ClaimToken, StringComparison.Ordinal))
            {
                return ValueTask.FromResult<JobRunRecord?>(null);
            }

            var createRun = command.CreateRun;
            if (createRun && schedule.ConcurrencyPolicy == ScheduleConcurrencyPolicy.SkipIfRunning)
            {
                createRun = !_runs.Values.Any(run =>
                    string.Equals(run.ScheduleId, schedule.Id, StringComparison.Ordinal)
                    && run.Phase is JobPhase.Pending or JobPhase.Running);
            }

            var scheduledFor = command.ScheduledFor.ToUniversalTime();
            JobRunRecord? run = null;
            if (createRun)
            {
                var existingOccurrence = _runs.Values.SingleOrDefault(candidate =>
                    string.Equals(candidate.ScheduleId, schedule.Id, StringComparison.Ordinal)
                    && candidate.ScheduledFor == scheduledFor);
                if (existingOccurrence is not null)
                {
                    run = existingOccurrence;
                }
                else
                {
                    if (_idempotency.TryGetValue(command.IdempotencyKey, out var existingRunId)
                        && _runs.TryGetValue(existingRunId, out var conflictingRun))
                    {
                        throw new IdempotencyConflictException(
                            command.IdempotencyKey,
                            conflictingRun.Id);
                    }

                    run = new JobRunRecord
                    {
                        Id = command.RunId,
                        JobKey = schedule.JobKey,
                        PayloadJson = schedule.PayloadJson,
                        Queue = schedule.Queue,
                        DeliveryProfile = ExecutionDeliveryProfile.Pull,
                        ExecutionLane = schedule.ExecutionLane,
                        ConsumerGroup = schedule.ConsumerGroup,
                        TransportId = null,
                        OrderingMode = schedule.OrderingMode,
                        Priority = schedule.Priority,
                        Phase = JobPhase.Pending,
                        AvailableAt = now,
                        CreatedAt = now,
                        MaxAttempts = schedule.MaxAttempts,
                        TimeoutSeconds = schedule.TimeoutSeconds,
                        ConcurrencyKey = schedule.ConcurrencyKey,
                        RetryPolicy = schedule.RetryPolicy,
                        IdempotencyKey = command.IdempotencyKey,
                        ScheduleId = schedule.Id,
                        ScheduledFor = scheduledFor,
                        OrderingSequence = ++_nextOrderingSequence
                    };

                    if (!_runs.TryAdd(run.Id, run))
                    {
                        throw new InvalidOperationException(
                            $"Schedule run id '{run.Id}' already exists for another occurrence.");
                    }

                    _idempotency[command.IdempotencyKey] = run.Id;
                    AddWorkAvailableOutbox(run, now);
                }
            }

            schedule.LastFireAt = scheduledFor;
            schedule.NextFireAt = command.NextFireAt.ToUniversalTime();
            schedule.ClaimToken = null;
            schedule.ClaimUntil = null;
            schedule.UpdatedAt = now;
            schedule.Version++;
            return ValueTask.FromResult(run);
        }
    }

    public ValueTask ReleaseClaimAsync(
        string scheduleId,
        string claimToken,
        DateTimeOffset retryAt,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_schedules.TryGetValue(scheduleId, out var schedule)
                && string.Equals(schedule.ClaimToken, claimToken, StringComparison.Ordinal))
            {
                schedule.ClaimToken = null;
                schedule.ClaimUntil = retryAt.ToUniversalTime();
                schedule.UpdatedAt = DateTimeOffset.UtcNow;
                schedule.Version++;
            }
        }

        return ValueTask.CompletedTask;
    }

    private static JobScheduleRecord CloneSchedule(JobScheduleRecord source) => new()
    {
        Id = source.Id,
        JobKey = source.JobKey,
        PayloadJson = source.PayloadJson,
        CronExpression = source.CronExpression,
        TimeZoneId = source.TimeZoneId,
        Queue = source.Queue,
        DeliveryProfile = source.DeliveryProfile,
        ExecutionLane = source.ExecutionLane,
        ConsumerGroup = source.ConsumerGroup,
        TransportId = source.TransportId,
        OrderingMode = source.OrderingMode,
        Priority = source.Priority,
        MisfirePolicy = source.MisfirePolicy,
        ConcurrencyPolicy = source.ConcurrencyPolicy,
        MaxAttempts = source.MaxAttempts,
        TimeoutSeconds = source.TimeoutSeconds,
        ConcurrencyKey = source.ConcurrencyKey,
        RetryPolicy = source.RetryPolicy,
        Enabled = source.Enabled,
        NextFireAt = source.NextFireAt,
        LastFireAt = source.LastFireAt,
        ClaimToken = source.ClaimToken,
        ClaimUntil = source.ClaimUntil,
        CreatedAt = source.CreatedAt,
        UpdatedAt = source.UpdatedAt,
        Version = source.Version
    };
}
