using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;

namespace KubeJob.Server.Runtime;

public sealed class DefaultJobClient : IJobClient
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly JobControlPlane _controlPlane;

    public DefaultJobClient(JobControlPlane controlPlane)
    {
        _controlPlane = controlPlane;
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

        if (job.IsEmpty)
        {
            throw new ArgumentException("The job key must be initialized.", nameof(job));
        }

        var payloadJson = JsonSerializer.Serialize(payload, SerializerOptions);
        var timeoutSeconds = checked((int)Math.Ceiling(options.Timeout.TotalSeconds));
        var receipt = await _controlPlane.SubmitAsync(
            new EnqueueJobRequest(
                job.Value,
                payloadJson,
                options.ResolveQueue(job.Value),
                options.Priority,
                options.NotBefore?.ToUniversalTime(),
                options.IdempotencyKey,
                options.ConcurrencyKey,
                options.MaxAttempts,
                timeoutSeconds,
                RetryPolicy: options.RetryPolicy,
                Continuation: options.Continuation,
                Compensation: options.Compensation),
            cancellationToken);

        return receipt.Handle;
    }

    public async ValueTask<IReadOnlyList<JobHandle>> EnqueueBatchAsync<TPayload>(
        JobKey<TPayload> job,
        IReadOnlyList<(TPayload Payload, JobEnqueueOptions? Options)> batch,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0) return Array.Empty<JobHandle>();
        if (job.IsEmpty)
            throw new ArgumentException("The job key must be initialized.", nameof(job));

        var requests = new EnqueueJobRequest[batch.Count];
        for (var i = 0; i < batch.Count; i++)
        {
            var (payload, options) = batch[i];
            var opts = options ?? new JobEnqueueOptions();
            var payloadJson = JsonSerializer.Serialize(payload, SerializerOptions);
            var timeoutSeconds = checked((int)Math.Ceiling(opts.Timeout.TotalSeconds));
            requests[i] = new EnqueueJobRequest(
                job.Value,
                payloadJson,
                opts.ResolveQueue(job.Value),
                opts.Priority,
                opts.NotBefore?.ToUniversalTime(),
                opts.IdempotencyKey,
                opts.ConcurrencyKey,
                opts.MaxAttempts,
                timeoutSeconds,
                RetryPolicy: opts.RetryPolicy,
                Continuation: opts.Continuation,
                Compensation: opts.Compensation);
        }

        var receipts = await _controlPlane.SubmitBatchAsync(requests, cancellationToken);
        var handles = new JobHandle[receipts.Count];
        for (var i = 0; i < receipts.Count; i++)
        {
            handles[i] = receipts[i].Handle;
        }
        return handles;
    }

    public async ValueTask<JobStatusSnapshot?> GetStatusAsync(
        string jobId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);

        return await _controlPlane.GetStatusAsync(jobId, cancellationToken);
    }

    public ValueTask<bool> CancelAsync(
        string jobId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);
        return _controlPlane.RequestCancelAsync(jobId, reason, cancellationToken);
    }
}
