using FluentAssertions;
using KubeJob.Core.Attributes;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;

namespace KubeJob.Tests.Generated;

public sealed record EchoPayload(string Value);

[KubeJob("generated.echo")]
public sealed class EchoJob : IKubeJob<EchoPayload>
{
    public ValueTask ExecuteAsync(
        EchoPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

public sealed class GeneratedJobKeyTests
{
    [Fact]
    public void Generator_creates_strongly_typed_job_key()
    {
        Jobs.Echo.Value.Should().Be("generated.echo");
        Jobs.Echo.IsEmpty.Should().BeFalse();
    }
}
