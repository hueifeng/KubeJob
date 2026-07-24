using KubeJob.Core.Attributes;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;

namespace KubeJob.PackageSmoke;

public sealed record EchoPayload(string Value);

[KubeJob("package.echo")]
public sealed class EchoJob : IKubeJob<EchoPayload>
{
    public ValueTask ExecuteAsync(
        EchoPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

public static class Program
{
    public static int Main()
    {
        return Jobs.Echo.Value == "package.echo" ? 0 : 1;
    }
}
