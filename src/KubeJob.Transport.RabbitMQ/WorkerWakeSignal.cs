namespace KubeJob.Transport.RabbitMQ;

public sealed class WorkerWakeSignal : IDisposable
{
    private readonly SemaphoreSlim _signal = new(0, 1);

    public void Pulse()
    {
        if (_signal.CurrentCount == 0)
        {
            try
            {
                _signal.Release();
            }
            catch (SemaphoreFullException)
            {
            }
        }
    }

    public Task<bool> WaitAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken) =>
        _signal.WaitAsync(timeout, cancellationToken);

    public void Dispose() => _signal.Dispose();
}
