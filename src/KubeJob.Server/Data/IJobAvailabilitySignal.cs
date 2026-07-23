namespace KubeJob.Server.Data;

public interface IJobAvailabilitySignal
{
    long Version { get; }
    ValueTask WaitForChangeAsync(long observedVersion, TimeSpan timeout, CancellationToken cancellationToken);
}

public sealed class PollingJobAvailabilitySignal : IJobAvailabilitySignal
{
    public long Version => 0;
    public async ValueTask WaitForChangeAsync(long observedVersion, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (timeout > TimeSpan.Zero) await Task.Delay(timeout, cancellationToken);
    }
}
