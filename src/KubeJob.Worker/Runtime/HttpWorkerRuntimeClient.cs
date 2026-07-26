using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Runtime;

public sealed class HttpWorkerRuntimeClient : IWorkerRuntimeClient, IDisposable
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _httpClient;

    public HttpWorkerRuntimeClient(IOptions<KubeJobWorkerOptions> options)
    {
        var workerOptions = options.Value;
        workerOptions.Validate();
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(workerOptions.ServerEndpoint, UriKind.Absolute),
            Timeout = TimeSpan.FromSeconds(30)
        };
    }

    public async ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        using var response = await _httpClient.PostAsJsonAsync(
            "api/kubejob/runtime/workers/register",
            request,
            SerializerOptions,
            cancellationToken);
        response.EnsureSuccessStatusCode();
        return await ReadRequiredAsync<RegisterWorkerSessionResponse>(response, cancellationToken);
    }

    public ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken) =>
        PostBooleanAsync("api/kubejob/runtime/workers/heartbeat", request, cancellationToken);

    public ValueTask<bool> CloseAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken) =>
        PostBooleanAsync("api/kubejob/runtime/workers/close", request, cancellationToken);

    public async ValueTask<ClaimJobsResponse> ClaimAsync(
        ClaimJobsRequest request,
        CancellationToken cancellationToken)
    {
        using var response = await _httpClient.PostAsJsonAsync(
            "api/kubejob/runtime/claims",
            request,
            SerializerOptions,
            cancellationToken);
        response.EnsureSuccessStatusCode();
        return await ReadRequiredAsync<ClaimJobsResponse>(response, cancellationToken);
    }

    public async ValueTask<AdmitExecutionResponse> AdmitAsync(
        AdmitExecutionRequest request,
        CancellationToken cancellationToken)
    {
        using var response = await _httpClient.PostAsJsonAsync(
            "api/kubejob/runtime/admissions",
            request,
            SerializerOptions,
            cancellationToken);
        response.EnsureSuccessStatusCode();
        return await ReadRequiredAsync<AdmitExecutionResponse>(response, cancellationToken);
    }

    public async ValueTask<RenewLeasesResponse> RenewLeasesAsync(
        RenewLeasesRequest request,
        CancellationToken cancellationToken)
    {
        using var response = await _httpClient.PostAsJsonAsync(
            "api/kubejob/runtime/leases/renew",
            request,
            SerializerOptions,
            cancellationToken);
        response.EnsureSuccessStatusCode();
        return await ReadRequiredAsync<RenewLeasesResponse>(response, cancellationToken);
    }

    public async ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken)
    {
        using var response = await _httpClient.PostAsJsonAsync(
            "api/kubejob/runtime/attempts/complete",
            request,
            SerializerOptions,
            cancellationToken);

        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            return await ReadRequiredAsync<CompleteAttemptResponse>(response, cancellationToken);
        }

        response.EnsureSuccessStatusCode();
        return await ReadRequiredAsync<CompleteAttemptResponse>(response, cancellationToken);
    }

    public ValueTask<bool> RequeueExecutionAsync(
        RequeueExecutionRequest request,
        CancellationToken cancellationToken) =>
        PostBooleanAsync(
            "api/kubejob/runtime/executions/requeue",
            request,
            cancellationToken);

    public void Dispose() => _httpClient.Dispose();

    private async ValueTask<bool> PostBooleanAsync<TRequest>(
        string path,
        TRequest request,
        CancellationToken cancellationToken)
    {
        using var response = await _httpClient.PostAsJsonAsync(
            path,
            request,
            SerializerOptions,
            cancellationToken);

        if (response.StatusCode == HttpStatusCode.Conflict
            || response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }

        response.EnsureSuccessStatusCode();
        return true;
    }

    private static async ValueTask<T> ReadRequiredAsync<T>(
        HttpResponseMessage response,
        CancellationToken cancellationToken) =>
        await response.Content.ReadFromJsonAsync<T>(SerializerOptions, cancellationToken)
        ?? throw new InvalidOperationException(
            $"KubeJob control plane returned an empty {typeof(T).Name} response.");
}
