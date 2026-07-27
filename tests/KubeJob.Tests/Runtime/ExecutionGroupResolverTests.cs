using FluentAssertions;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Locks in the configurable execution-group resolution policy (L6).
/// </summary>
public sealed class ExecutionGroupResolverTests
{
    [Fact]
    public void Default_group_is_used_when_no_per_queue_mapping_is_configured()
    {
        var resolver = new DefaultExecutionGroupResolver(Options.Create(new QueueDeliveryOptions
        {
            DefaultExecutionGroup = "region-a"
        }));

        resolver.Resolve("orders.push").Should().Be("region-a");
        resolver.Resolve("reports.generate").Should().Be("region-a");
    }

    [Fact]
    public void Per_queue_mapping_overrides_the_default_group()
    {
        var options = new QueueDeliveryOptions
        {
            DefaultExecutionGroup = "region-a"
        };
        options.QueueGroups["reports.generate"] = "region-b";
        var resolver = new DefaultExecutionGroupResolver(Options.Create(options));

        resolver.Resolve("orders.push").Should().Be("region-a");
        resolver.Resolve("reports.generate").Should().Be("region-b");
    }

    [Fact]
    public void Missing_configuration_falls_back_to_legacy_default_group()
    {
        var resolver = new DefaultExecutionGroupResolver(Options.Create(new QueueDeliveryOptions()));

        resolver.Resolve("orders.push").Should().Be("default");
    }

    [Fact]
    public void Validate_rejects_empty_queue_or_group_identifiers()
    {
        var options = new QueueDeliveryOptions();
        options.QueueGroups[" "] = "region-a";
        var action = () => options.Validate();
        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*empty logical queue*");

        options.QueueGroups.Clear();
        options.QueueGroups["orders.push"] = "   ";
        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*empty group identifier*");
    }

    [Fact]
    public void Validate_rejects_an_overlong_default_group()
    {
        var options = new QueueDeliveryOptions
        {
            DefaultExecutionGroup = new string('x', 201)
        };
        var action = () => options.Validate();
        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*DefaultExecutionGroup*");
    }
}
