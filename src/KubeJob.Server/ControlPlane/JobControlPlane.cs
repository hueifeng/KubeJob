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
    private readonly IExecutionGroupResolver _executionGroupResolver;
    private readonly JobRuntimeOptions _options;
    private readonly KubeJobControlPlaneMetrics? _metrics;

    public JobControlPlane(
        IJobSubmissionStore submissions,
        IJobQueryStore queries,
        IQueueRouter queueRouter,
        IExecutionGroupResolver executionGroupResolver,
        IOptions<JobRuntimeOptions> options,
        KubeJobControlPlaneMetrics? metrics = null)
    {
        _submissions = submissions;
        _queries = queries;
        _queueRouter = queueRouter;
        _executionGroupResolver = executionGroupResolver;
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

        var result = await _submissions.SubmitAsync(
            new SubmitJobCommand(
                request.JobKey,
                request.PayloadJson,
                request.Queue,
                request.Priority,
                (request.NotBefore ?? DateTimeOffset.UtcNow).ToUniversalTime(),
                request.IdempotencyKey,
                request.ConcurrencyKey,
                request.MaxAttempts,
                request.TimeoutSeconds,
                DeliveryTarget: route.Target),
            cancellationToken);

        _metrics?.SubmissionCompleted(result.Existing);
        if (activity?.IsAllDataRequested == true)
        {
            activity.SetTag("kubejob.idempotency.existing", result.Existing);
        }

        return new JobSubmissionReceipt(new JobHandle(result.Run.Id), result.Existing);
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
                && _queueRouter.Resolve(run.Queue).Target.Profile == ExecutionDeliveryProfile.BrokerDispatch)
            {
                group = _executionGroupResolver.Resolve(run.Queue);
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
        run.FailureMessage);

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
