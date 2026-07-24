using KubeJob.Core.Attributes;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;

namespace KubeJob.GeneratorNegative;

public sealed record FirstPayload(string Value);
public sealed record SecondPayload(string Value);

[KubeJob("duplicate.key")]
public sealed class FirstJob : IKubeJob<FirstPayload>
{
    public ValueTask ExecuteAsync(
        FirstPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

[KubeJob("duplicate.key")]
public sealed class SecondJob : IKubeJob<SecondPayload>
{
    public ValueTask ExecuteAsync(
        SecondPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken) => ValueTask.CompletedTask;
}
