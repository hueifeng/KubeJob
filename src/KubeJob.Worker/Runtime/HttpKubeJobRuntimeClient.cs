using System.Net.Http.Json;
using KubeJob.Core.Dtos;

namespace KubeJob.Worker.Runtime;

public sealed class HttpKubeJobRuntimeClient : IKubeJobRuntimeClient
{
    private readonly HttpClient _client;
    public HttpKubeJobRuntimeClient(HttpClient client) => _client = client;

    public async Task<RegisterWorkerSessionResponse> RegisterAsync(RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken)
    {
        using var response = await _client.PostAsJsonAsync("api/kubejob/runtime/register", request,
            KubeJobRuntimeJsonContext.Default.RegisterWorkerSessionRequest, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync(
            KubeJobRuntimeJsonContext.Default.RegisterWorkerSessionResponse, cancellationToken)
            ?? throw new InvalidOperationException("Empty registration response.");
    }

    public async Task<ClaimRunsResponse> ClaimAsync(ClaimRunsRequest request, CancellationToken cancellationToken)
    {
        using var response = await _client.PostAsJsonAsync("api/kubejob/runtime/claim", request,
            KubeJobRuntimeJsonContext.Default.ClaimRunsRequest, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync(
            KubeJobRuntimeJsonContext.Default.ClaimRunsResponse, cancellationToken) ?? new ClaimRunsResponse();
    }

    public async Task<RenewLeasesResponse> RenewAsync(RenewLeasesRequest request, CancellationToken cancellationToken)
    {
        using var response = await _client.PostAsJsonAsync("api/kubejob/runtime/renew", request,
            KubeJobRuntimeJsonContext.Default.RenewLeasesRequest, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync(
            KubeJobRuntimeJsonContext.Default.RenewLeasesResponse, cancellationToken) ?? new RenewLeasesResponse();
    }

    public async Task<bool> CompleteAsync(CompleteRunRequest request, CancellationToken cancellationToken)
    {
        using var response = await _client.PostAsJsonAsync("api/kubejob/runtime/complete", request,
            KubeJobRuntimeJsonContext.Default.CompleteRunRequest, cancellationToken);
        return response.IsSuccessStatusCode;
    }
}
