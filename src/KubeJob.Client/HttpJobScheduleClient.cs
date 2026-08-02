using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;

namespace KubeJob.Client;

public sealed class HttpJobScheduleClient : IJobScheduleClient
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _httpClient;

    public HttpJobScheduleClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async ValueTask<JobScheduleHandle> UpsertCronAsync<TPayload>(
        string scheduleId,
        JobKey<TPayload> job,
        TPayload payload,
        string cronExpression,
        CronScheduleOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        ArgumentException.ThrowIfNullOrWhiteSpace(cronExpression);
        if (job.IsEmpty)
        {
            throw new ArgumentException("The job key must be initialized.", nameof(job));
        }

        options ??= new CronScheduleOptions();
        var request = new UpsertCronScheduleRequest(
            job.Value,
            JsonSerializer.Serialize(payload, SerializerOptions),
            cronExpression,
            options.TimeZoneId,
            options.ResolveQueue(job.Value),
            options.Priority,
            options.MisfirePolicy,
            options.ConcurrencyPolicy,
            options.MaxAttempts,
            checked((int)Math.Ceiling(options.Timeout.TotalSeconds)),
            options.Enabled,
            options.ConcurrencyKey,
            options.RetryPolicy,
            options.Continuation,
            options.Compensation);

        using var response = await _httpClient.PutAsJsonAsync(
            $"api/kubejob/schedules/{Uri.EscapeDataString(scheduleId)}",
            request,
            SerializerOptions,
            cancellationToken);
        response.EnsureSuccessStatusCode();
        return new JobScheduleHandle(scheduleId);
    }

    public async ValueTask<JobScheduleSnapshot?> GetAsync(
        string scheduleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        using var response = await _httpClient.GetAsync(
            $"api/kubejob/schedules/{Uri.EscapeDataString(scheduleId)}",
            cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<JobScheduleSnapshot>(
            SerializerOptions,
            cancellationToken);
    }

    public async ValueTask<bool> SetEnabledAsync(
        string scheduleId,
        bool enabled,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        using var response = await _httpClient.PostAsJsonAsync(
            $"api/kubejob/schedules/{Uri.EscapeDataString(scheduleId)}/enabled",
            new SetScheduleEnabledRequest(enabled),
            SerializerOptions,
            cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }

        response.EnsureSuccessStatusCode();
        return true;
    }

    public async ValueTask<bool> DeleteAsync(
        string scheduleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);
        using var response = await _httpClient.DeleteAsync(
            $"api/kubejob/schedules/{Uri.EscapeDataString(scheduleId)}",
            cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }

        response.EnsureSuccessStatusCode();
        return true;
    }
}
