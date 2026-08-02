using System.Text;
using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Telemetry;
using KubeJob.ControlPlane.Telemetry;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.ControlPlane;

public sealed record JobSubmissionReceipt(JobHandle Handle, bool Existing);

/// <summary>
/// Owns logical job submission and observation rules independently of HTTP,
/// typed client serialization, and future message-ingress adapters.
/// </summary>
public sealed class JobControlPlane
{
    private readonly IJobSubmissionStore _submissions;
    private readonly IJobQueryStore _queries;
    private readonly IQueueRouter _queueRouter;
    private readonly OutboxPublisherSignal _wake;
    private readonly JobRuntimeOptions _options;
    private readonly KubeJobControlPlaneMetrics? _metrics;

    public JobControlPlane(
        IJobSubmissionStore submissions,
        IJobQueryStore queries,
        IQueueRouter queueRouter,
        IOptions<JobRuntimeOptions> options,
        OutboxPublisherSignal wake,
        KubeJobControlPlaneMetrics? metrics = null)
    {
        ArgumentNullException.ThrowIfNull(wake);
        _submissions = submissions;
        _queries = queries;
        _queueRouter = queueRouter;
        _wake = wake;
        _options = options.Value;
        _metrics = metrics;
    }

    public async ValueTask<JobSubmissionReceipt> SubmitAsync(
        EnqueueJobRequest request,
        CancellationToken cancellationToken = default)
    {
        ValidateSubmission(request);
        using var activity = KubeJobTelemetry.ActivitySource.StartActivity("kubejob.submit");

        var route = _queueRouter.Resolve(request.Queue);
        ValidateOrdering(request, route.Target);
        request = NormalizeAndValidateTerminalActions(request, route.Queue);

        var result = await _submissions.SubmitAsync(
            new SubmitJobCommand(
                request.JobKey,
                request.PayloadJson,
                route.Queue,
                request.Priority,
                (request.NotBefore ?? DateTimeOffset.UtcNow).ToUniversalTime(),
                request.IdempotencyKey,
                request.ConcurrencyKey,
                request.MaxAttempts,
                request.TimeoutSeconds,
                DeliveryTarget: route.Target,
                RetryPolicy: request.RetryPolicy,
                Continuation: request.Continuation,
                Compensation: request.Compensation),
            cancellationToken);

        _metrics?.SubmissionCompleted(result.Existing);
        if (activity?.IsAllDataRequested == true)
        {
            activity.SetTag("kubejob.idempotency.existing", result.Existing);
        }

        // Wake the in-process OutboxPublisherService so the new row is
        // claimed on the next dispatch iteration instead of waiting for the
        // OutboxPollInterval tick. Idempotent dedupes (Existing == true)
        // don't enqueue a new outbox row, so they don't need to wake.
        if (!result.Existing)
        {
            _wake.Signal();
        }

        return new JobSubmissionReceipt(new JobHandle(result.Run.Id), result.Existing);
    }

    public async ValueTask<IReadOnlyList<JobSubmissionReceipt>> SubmitBatchAsync(
        IReadOnlyList<EnqueueJobRequest> requests,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(requests);
        ValidateSubmissionBatchSize(requests.Count);
        if (requests.Count == 0)
        {
            return Array.Empty<JobSubmissionReceipt>();
        }

        var commands = new SubmitJobCommand[requests.Count];
        for (var index = 0; index < requests.Count; index++)
        {
            var request = requests[index];
            if (request is null)
            {
                throw new ControlPlaneValidationException(
                    "invalid_job_submission",
                    $"Submission batch item at index {index} cannot be null.");
            }
            ValidateSubmission(request);
            var route = _queueRouter.Resolve(request.Queue);
            ValidateOrdering(request, route.Target);
            request = NormalizeAndValidateTerminalActions(request, route.Queue);
            commands[index] = new SubmitJobCommand(
                request.JobKey,
                request.PayloadJson,
                route.Queue,
                request.Priority,
                (request.NotBefore ?? DateTimeOffset.UtcNow).ToUniversalTime(),
                request.IdempotencyKey,
                request.ConcurrencyKey,
                request.MaxAttempts,
                request.TimeoutSeconds,
                DeliveryTarget: route.Target,
                RetryPolicy: request.RetryPolicy,
                Continuation: request.Continuation,
                Compensation: request.Compensation);
        }

        using var activity = KubeJobTelemetry.ActivitySource.StartActivity("kubejob.submit_batch");
        var results = await _submissions.SubmitBatchAsync(commands, cancellationToken);
        var receipts = new JobSubmissionReceipt[results.Count];
        for (var index = 0; index < results.Count; index++)
        {
            var result = results[index];
            _metrics?.SubmissionCompleted(result.Existing);
            receipts[index] = new JobSubmissionReceipt(new JobHandle(result.Run.Id), result.Existing);
        }

        if (activity?.IsAllDataRequested == true)
        {
            activity.SetTag("kubejob.submit_batch.count", results.Count);
        }

        // Wake once per batch — coalescing in OutboxPublisherSignal means a
        // single Signal() collapses any number of additional concurrent
        // writers into one extra scan. Only signal if at least one result
        // produced a new row.
        if (results.Any(r => !r.Existing))
        {
            _wake.Signal();
        }

        return receipts;
    }

    /// <summary>
    /// Applies the same batch boundary before an adapter allocates its
    /// translated command array. Persistence remains the authoritative second
    /// check in <see cref="SubmitBatchAsync"/>.
    /// </summary>
    public void ValidateSubmissionBatchSize(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count > _options.MaxSubmissionBatchSize)
        {
            throw new ControlPlaneValidationException(
                "job_submission_batch_too_large",
                $"A submission batch cannot contain more than {_options.MaxSubmissionBatchSize} jobs.");
        }
    }

    public async ValueTask<JobStatusSnapshot?> GetStatusAsync(
        string runId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        var run = await _queries.GetRunAsync(runId, cancellationToken);
        return run is null ? null : ToSnapshot(run);
    }

    public async ValueTask<bool> RequestCancelAsync(
        string runId,
        string? reason,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);

        string? group = null;
        if (_options.BrokerCancelPropagationEnabled)
        {
            var run = await _queries.GetRunAsync(runId, cancellationToken);
            if (run is not null
                && run.DeliveryProfile == ExecutionDeliveryProfile.BrokerDispatch)
            {
                // Use the group persisted on the Run at submission time. The
                // routing config may have changed since submission; inspecting
                // it here could fan the cancel marker out to the wrong group.
                group = run.ConsumerGroup;
            }
        }

        var result = await _submissions.RequestCancelAsync(runId, reason, group, cancellationToken);
        return result.Requested;
    }

    public async ValueTask<IReadOnlyList<JobAttemptSnapshot>?> GetAttemptsAsync(
        string runId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        if (await _queries.GetRunAsync(runId, cancellationToken) is null)
        {
            return null;
        }

        var attempts = await _queries.GetAttemptsAsync(runId, cancellationToken);
        return attempts.Select(ToSnapshot).ToArray();
    }

    private void ValidateSubmission(EnqueueJobRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (string.IsNullOrWhiteSpace(request.JobKey)
            || string.IsNullOrWhiteSpace(request.PayloadJson)
            || string.IsNullOrWhiteSpace(request.Queue)
            || request.MaxAttempts < 1
            || request.TimeoutSeconds is < 1 or > 86_400)
        {
            throw new ControlPlaneValidationException(
                "invalid_job_submission",
                "JobKey, valid payload JSON, queue, positive MaxAttempts, and TimeoutSeconds between 1 and 86400 are required.");
        }

        var overlongField = request.JobKey.Length > 300
            ? "JobKey"
            : request.Queue.Length > 100
                ? "Queue"
                : request.IdempotencyKey?.Length > 500
                    ? "IdempotencyKey"
                    : request.ConcurrencyKey?.Length > 500
                        ? "ConcurrencyKey"
                        : null;
        if (overlongField is not null)
        {
            throw new ControlPlaneValidationException(
                "job_submission_field_too_long",
                $"{overlongField} exceeds the maximum storage length.");
        }

        if (Encoding.UTF8.GetByteCount(request.PayloadJson) > _options.MaxPayloadBytes)
        {
            throw new ControlPlaneValidationException(
                "job_payload_too_large",
                $"PayloadJson exceeds the configured maximum of {_options.MaxPayloadBytes} UTF-8 bytes.");
        }

        try
        {
            using var document = JsonDocument.Parse(request.PayloadJson);
        }
        catch (JsonException)
        {
            throw new ControlPlaneValidationException(
                "invalid_job_payload",
                "PayloadJson must contain valid JSON.");
        }

        if (request.RetryPolicy is { } retryPolicy)
        {
            try
            {
                retryPolicy.Validate();
            }
            catch (Exception exception) when (exception is ArgumentException or InvalidOperationException)
            {
                throw new ControlPlaneValidationException(
                    "invalid_job_retry_policy",
                    exception.Message);
            }
        }
    }

    private EnqueueJobRequest NormalizeAndValidateTerminalActions(
        EnqueueJobRequest request,
        string canonicalQueue)
    {
        var normalized = TerminalActionValidator.NormalizeAndValidate(
            request.Continuation,
            request.Compensation,
            canonicalQueue,
            _options.MaxPayloadBytes,
            "invalid_job_terminal_action",
            "job_terminal_action_payload_too_large");
        return request with
        {
            Continuation = normalized.Continuation,
            Compensation = normalized.Compensation
        };
    }

    private static void ValidateOrdering(EnqueueJobRequest request, DeliveryTarget target)
    {
        if (target.OrderingMode == ExecutionOrderingMode.KeyOrdered
            && string.IsNullOrWhiteSpace(request.ConcurrencyKey))
        {
            throw new ControlPlaneValidationException(
                "ordering_key_required",
                "KeyOrdered queues require a non-empty ConcurrencyKey as the partition key.");
        }
    }

    private static JobStatusSnapshot ToSnapshot(JobRunRecord run) => new(
        run.Id,
        run.Phase,
        run.AttemptCount,
        run.CreatedAt,
        run.StartedAt,
        run.CompletedAt,
        run.CurrentWorkerId,
        run.FailureCode,
        run.FailureMessage,
        run.ParentRunId,
        run.RelationKind);

    private static JobAttemptSnapshot ToSnapshot(JobAttemptRecord attempt) => new(
        attempt.Id,
        attempt.AttemptNumber,
        attempt.WorkerId,
        attempt.SessionId,
        attempt.SessionEpoch,
        attempt.Phase,
        attempt.ClaimedAt,
        attempt.StartedAt,
        attempt.LeaseExpiresAt,
        attempt.CompletedAt,
        attempt.FailureCode,
        attempt.FailureMessage);
}
