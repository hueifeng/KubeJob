using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;

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

    public JobControlPlane(
        IJobSubmissionStore submissions,
        IJobQueryStore queries)
    {
        _submissions = submissions;
        _queries = queries;
    }

    public async ValueTask<JobSubmissionReceipt> SubmitAsync(
        EnqueueJobRequest request,
        CancellationToken cancellationToken = default)
    {
        ValidateSubmission(request);

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
                request.TimeoutSeconds),
            cancellationToken);

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

    public ValueTask<bool> RequestCancelAsync(
        string runId,
        string? reason,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        return _submissions.RequestCancelAsync(runId, reason, cancellationToken);
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

    private static void ValidateSubmission(EnqueueJobRequest request)
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
