using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;

namespace KubeJob.Client;

public sealed class HttpJobClient : IJobClient
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _httpClient;

    public HttpJobClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public ValueTask<JobHandle> EnqueueAsync<TPayload>(
        JobKey<TPayload> job,
        TPayload payload,
        CancellationToken cancellationToken = default) =>
        EnqueueAsync(job, payload, new JobEnqueueOptions(), cancellationToken);

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

        var request = CreateRequest(job, payload, options);

        using var response = await _httpClient.PostAsJsonAsync(
            "api/kubejob/jobs",
            request,
            SerializerOptions,
            cancellationToken);

        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            var conflict = await response.Content.ReadFromJsonAsync<IdempotencyConflictPayload>(
                SerializerOptions,
                cancellationToken);
            throw new IdempotencyConflictException(
                conflict?.IdempotencyKey ?? options.IdempotencyKey ?? string.Empty,
                conflict?.ExistingJobId ?? string.Empty);
        }

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<JobHandle>(
                   SerializerOptions,
                   cancellationToken)
               ?? throw new InvalidOperationException("KubeJob enqueue returned an empty response.");
    }

    public async ValueTask<IReadOnlyList<JobHandle>> EnqueueBatchAsync<TPayload>(
        JobKey<TPayload> job,
        IReadOnlyList<(TPayload Payload, JobEnqueueOptions? Options)> batch,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0) return Array.Empty<JobHandle>();
        if (job.IsEmpty)
        {
            throw new ArgumentException("The job key must be initialized.", nameof(job));
        }

        var requests = new EnqueueJobRequest[batch.Count];
        for (var i = 0; i < batch.Count; i++)
        {
            var (payload, options) = batch[i];
            requests[i] = CreateRequest(job, payload, options ?? new JobEnqueueOptions());
        }

        using var response = await _httpClient.PostAsJsonAsync(
            "api/kubejob/jobs/batch",
            requests,
            SerializerOptions,
            cancellationToken);

        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            var conflict = await response.Content.ReadFromJsonAsync<IdempotencyConflictPayload>(
                SerializerOptions,
                cancellationToken);
            throw new IdempotencyConflictException(
                conflict?.IdempotencyKey ?? string.Empty,
                conflict?.ExistingJobId ?? string.Empty);
        }

        response.EnsureSuccessStatusCode();
        var handles = await response.Content.ReadFromJsonAsync<JobHandle[]>(
            SerializerOptions,
            cancellationToken);
        if (handles is null || handles.Length != batch.Count)
        {
            throw new InvalidOperationException(
                "KubeJob batch enqueue returned an unexpected handle count.");
        }

        return handles;
    }

    public async ValueTask<JobStatusSnapshot?> GetStatusAsync(
        string jobId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);
        using var response = await _httpClient.GetAsync(
            $"api/kubejob/jobs/{Uri.EscapeDataString(jobId)}",
            cancellationToken);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<JobStatusSnapshot>(
            SerializerOptions,
            cancellationToken);
    }

    public async ValueTask<bool> CancelAsync(
        string jobId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);
        using var response = await _httpClient.PostAsJsonAsync(
            $"api/kubejob/jobs/{Uri.EscapeDataString(jobId)}/cancel",
            new CancelJobRequest(reason),
            SerializerOptions,
            cancellationToken);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }

        response.EnsureSuccessStatusCode();
        return true;
    }

    private sealed record IdempotencyConflictPayload(
        string Code,
        string IdempotencyKey,
        string ExistingJobId);

    private static EnqueueJobRequest CreateRequest<TPayload>(
        JobKey<TPayload> job,
        TPayload payload,
        JobEnqueueOptions options) => new(
            job.Value,
            JsonSerializer.Serialize(payload, SerializerOptions),
            options.ResolveQueue(job.Value),
            options.Priority,
            options.NotBefore?.ToUniversalTime(),
            options.IdempotencyKey,
            options.ConcurrencyKey,
            options.MaxAttempts,
            checked((int)Math.Ceiling(options.Timeout.TotalSeconds)),
            RetryPolicy: options.RetryPolicy);
}
