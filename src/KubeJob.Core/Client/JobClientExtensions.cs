namespace KubeJob.Core.Client;

public static class JobClientExtensions
{
    public static async ValueTask<JobStatusSnapshot> WaitForCompletionAsync(
        this IJobClient client,
        JobHandle handle,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default) =>
        await client.WaitForCompletionAsync(
            handle.JobId,
            pollInterval,
            cancellationToken);

    public static async ValueTask<JobStatusSnapshot> WaitForCompletionAsync(
        this IJobClient client,
        string jobId,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);

        var delay = pollInterval ?? TimeSpan.FromMilliseconds(500);
        if (delay < TimeSpan.FromMilliseconds(50)
            || delay > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(
                nameof(pollInterval),
                "Poll interval must be between 50 milliseconds and 30 seconds.");
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var status = await client.GetStatusAsync(jobId, cancellationToken)
                ?? throw new KeyNotFoundException($"KubeJob run '{jobId}' was not found.");

            if (status.Phase is
                JobPhase.Succeeded or
                JobPhase.Failed or
                JobPhase.Canceled or
                JobPhase.Dead)
            {
                return status;
            }

            await Task.Delay(delay, cancellationToken);
        }
    }
}
