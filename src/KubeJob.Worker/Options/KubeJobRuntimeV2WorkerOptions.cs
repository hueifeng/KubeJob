namespace KubeJob.Worker.Options;

public sealed class KubeJobRuntimeV2WorkerOptions
{
    public string ServerEndpoint { get; set; } = "http://localhost:5000/";
    public int MaxConcurrentJobs { get; set; } = Math.Max(1, Environment.ProcessorCount);
    public string WorkerId { get; set; } = Environment.MachineName;
    public Dictionary<string, string> Labels { get; set; } = new(StringComparer.Ordinal);
    public string[] QueueNames { get; set; } = Array.Empty<string>();
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan LongPollTimeout { get; set; } = TimeSpan.FromSeconds(20);
    public TimeSpan MinEmptyClaimDelay { get; set; } = TimeSpan.FromMilliseconds(50);
    public TimeSpan MaxEmptyClaimDelay { get; set; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan ShutdownGracePeriod { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxServerClaimBatch { get; set; } = 256;
}
