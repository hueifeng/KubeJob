using System.Runtime.CompilerServices;

namespace KubeJob.Tests;

/// <summary>
/// The GitHub-hosted 2-vCPU runner starves a default-sized thread pool while
/// the suite's hosts churn RabbitMQ connections and hosted-service loops
/// (RabbitMQ.Client's synchronous API blocks on async work, so a MinThreads
/// of 2 can deadlock it). Raise the floor when the test assembly loads.
/// </summary>
internal static class ThreadPoolInitializer
{
    [ModuleInitializer]
    internal static void Initialize()
    {
        var workers = Math.Max(8, Environment.ProcessorCount * 4);
        ThreadPool.SetMinThreads(workers, workers);
    }
}
