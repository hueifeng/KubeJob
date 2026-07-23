using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;

namespace KubeJob.Server.Runtime;

public sealed class DefaultJobClient : IJobClient
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly IJobSubmissionStore _submissionStore;
    private readonly IJobQueryStore _queryStore;

    public DefaultJobClient(
        IJobSubmissionStore submissionStore,
        IJobQueryStore queryStore)
    {
        _submissionStore = submissionStore;
        _queryStore = queryStore;
    }

    public ValueTask<JobHandle> EnqueueAsync<TPayload>(
        JobKey<TPayload> job,
        TPayload payload,
        CancellationToken cancellationToken = default)
    {
        return EnqueueAsync(job, payload, new JobEnqueueOptions(), cancellationToken);
    }

    public async ValueTask<JobHandle> EnqueueAsync<TPayload>(
        JobKey<TPayload> job,
        TPayload payload,
        JobEnqueueOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        if (job.IsEmpty)
        {
            throw new ArgumentException("The job key must be initialized.", nameof(job));
        }

        var payloadJson = JsonSerializer.Serialize(payload, SerializerOptions);
        var availableAt = options.NotBefore ?? DateTimeOffset.UtcNow;
        var timeoutSeconds = checked((int)Math.Ceiling(options.Timeout.TotalSeconds));

        var result = await _submissionStore.SubmitAsync(
            new SubmitJobCommand(
                job.Value,
                payloadJson,
                options.Queue,
                options.Priority,
                availableAt,
                options.IdempotencyKey,
                options.ConcurrencyKey,
                options.MaxAttempts,
                timeoutSeconds),
            cancellationToken);

        return new JobHandle(result.Run.Id);
    }

    public async ValueTask<JobStatusSnapshot?> GetStatusAsync(
        string jobId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);

        var run = await _queryStore.GetRunAsync(jobId, cancellationToken);
        if (run is null)
        {
            return null;
        }

        return new JobStatusSnapshot(
            run.Id,
            run.Phase,
            run.AttemptCount,
            run.CreatedAt,
            run.StartedAt,
            run.CompletedAt,
            run.CurrentWorkerId,
            run.FailureCode,
            run.FailureMessage);
    }

    public ValueTask<bool> CancelAsync(
        string jobId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);
        return _submissionStore.RequestCancelAsync(jobId, reason, cancellationToken);
    }
}
