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
        options.Validate();

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
                options.Queue,
                options.Priority,
                options.NotBefore?.ToUniversalTime(),
                options.IdempotencyKey,
                options.ConcurrencyKey,
                options.MaxAttempts,
                timeoutSeconds),
            cancellationToken);

        return receipt.Handle;
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
