using KubeJob.Core.Attributes;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Scheduling;

namespace KubeJob.MetaPackageSmoke;

public sealed record MetaPayload(string Value);

[KubeJob("package.meta")]
public sealed class MetaJob : IKubeJob<MetaPayload>
{
    public ValueTask ExecuteAsync(
        MetaPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

public static class Program
{
    public static int Main()
    {
        JobKeyAssertions();
        ApiSurfaceAssertions();
        return 0;
    }

    private static void JobKeyAssertions()
    {
        if (Jobs.Meta.Value != "package.meta")
        {
            throw new InvalidOperationException("Typed key was not generated through the KubeJob meta package.");
        }
    }

    private static void ApiSurfaceAssertions()
    {
        _ = typeof(IJobClient);
        _ = typeof(IJobScheduleClient);
        _ = typeof(CronScheduleOptions);
        _ = typeof(global::KubeJob.UnifiedHostingExtensions);
    }
}
