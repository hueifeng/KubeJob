using FluentAssertions;
using KubeJob.Worker.Extensions;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Tests.Generated;

public sealed class GeneratedRegistrationTests
{
    [Fact]
    public void Attribute_key_drives_both_generated_key_and_handler_registry()
    {
        var services = new ServiceCollection();
        services.AddKubeJobHandler<EchoJob, EchoPayload>();

        using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<JobHandlerRegistry>();

        Jobs.Echo.Value.Should().Be("generated.echo");
        registry.Capabilities.Should().ContainSingle("generated.echo");
    }
}
