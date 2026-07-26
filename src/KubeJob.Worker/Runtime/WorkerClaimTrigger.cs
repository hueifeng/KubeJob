namespace KubeJob.Worker.Runtime;

/// <summary>
/// Controls how an idle worker waits before attempting another authoritative
/// claim. The timeout keeps polling as a liveness fallback; optional transport
/// adapters may pulse the same trigger to reduce latency.
/// </summary>
public interface IWorkerClaimTrigger
{
    Task WaitAsync(TimeSpan maximumDelay, CancellationToken cancellationToken);
}

/// <summary>
/// Allows an optional local transport listener to wake the worker claim loop.
/// Repeated pulses are deliberately coalesced.
/// </summary>
public interface IWorkerClaimTriggerSource
{
    void Pulse();
}

public sealed class WorkerClaimTrigger :
    IWorkerClaimTrigger,
    IWorkerClaimTriggerSource,
    IDisposable
{
    private readonly SemaphoreSlim _signal = new(0, 1);

    public void Pulse()
    {
        if (_signal.CurrentCount != 0)
        {
            return;
        }

        try
        {
            _signal.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    public async Task WaitAsync(
        TimeSpan maximumDelay,
        CancellationToken cancellationToken)
    {
        if (maximumDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumDelay),
                maximumDelay,
                "The maximum claim delay must be positive.");
        }

        await _signal.WaitAsync(maximumDelay, cancellationToken);
    }

    public void Dispose() => _signal.Dispose();
}
