using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Decorates the remote HTTP protocol with an asynchronous wake signal. The
/// second Claim is still authoritative; RabbitMQ never grants ownership.
/// </summary>
public sealed class NotificationAwareWorkerRuntimeClient : IWorkerRuntimeClient
{
    private readonly HttpWorkerRuntimeClient _inner;
    private readonly WorkerWakeSignal _signal;
    private readonly KubeJobWorkerOptions _options;

    public NotificationAwareWorkerRuntimeClient(
        HttpWorkerRuntimeClient inner,
        WorkerWakeSignal signal,
        IOptions<KubeJobWorkerOptions> options)
    {
        _inner = inner;
        _signal = signal;
        _options = options.Value;
    }

    public ValueTask<RegisterWorkerSessionResponse> RegisterAsync(
        RegisterWorkerSessionRequest request,
        CancellationToken cancellationToken) =>
        _inner.RegisterAsync(request, cancellationToken);

    public ValueTask<bool> HeartbeatAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken) =>
        _inner.HeartbeatAsync(request, cancellationToken);

    public ValueTask<bool> CloseAsync(
        WorkerHeartbeatRequest request,
        CancellationToken cancellationToken) =>
        _inner.CloseAsync(request, cancellationToken);

    public async ValueTask<ClaimJobsResponse> ClaimAsync(
        ClaimJobsRequest request,
        CancellationToken cancellationToken)
    {
        var first = await _inner.ClaimAsync(request, cancellationToken);
        if (first.Jobs.Count > 0)
        {
            return first;
        }

        var signaled = await _signal.WaitAsync(
            _options.EmptyPollDelay,
            cancellationToken);
        return signaled
            ? await _inner.ClaimAsync(request, cancellationToken)
            : first;
    }

    public ValueTask<RenewLeasesResponse> RenewLeasesAsync(
        RenewLeasesRequest request,
        CancellationToken cancellationToken) =>
        _inner.RenewLeasesAsync(request, cancellationToken);

    public ValueTask<CompleteAttemptResponse> CompleteAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken) =>
        _inner.CompleteAsync(request, cancellationToken);
}
