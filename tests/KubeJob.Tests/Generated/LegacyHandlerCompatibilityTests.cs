using FluentAssertions;
using KubeJob.Core.Attributes;
using KubeJob.Core.Context;
using KubeJob.Core.Interfaces;

namespace KubeJob.Tests.Generated;

[KubeJob("legacy.compatible")]
public sealed class LegacyCompatibleJob : IKubeJob
{
    public Task ExecuteAsync(
        KubeJobContext context,
        CancellationToken token) => Task.CompletedTask;
}

public sealed class LegacyHandlerCompatibilityTests
{
    [Fact]
    public void Legacy_handler_remains_constructible()
    {
        new LegacyCompatibleJob().Should().NotBeNull();
    }
}
