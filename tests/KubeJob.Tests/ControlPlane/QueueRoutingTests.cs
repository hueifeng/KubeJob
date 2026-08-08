using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.ControlPlane;

public sealed class QueueRoutingTests
{
    [Fact]
    public void Unconfigured_logical_queue_uses_postgres_pull_by_default()
    {
        var options = Options.Create(new QueueDeliveryOptions());
        var router = new ConfigurationQueueRouter(options);

        var route = router.Resolve("orders.push");

        route.Queue.Should().Be("orders.push");
        route.Target.Profile.Should().Be(ExecutionDeliveryProfile.Pull);
        route.Target.TransportId.Should().BeNull();
        route.Target.ConsumerGroup.Should().Be("default");
        route.Target.ExecutionLane.Should().Be("default");
        route.Target.OrderingMode.Should().Be(ExecutionOrderingMode.Parallel);
    }

    [Fact]
    public void Per_queue_definition_only_changes_managed_policy()
    {
        var options = new QueueDeliveryOptions();
        options.Queues["orders.push"] = new QueueDefinition
        {
            ConsumerGroup = "region-b",
            ExecutionLane = "cpu",
            OrderingMode = ExecutionOrderingMode.KeyOrdered
        };
        var router = new ConfigurationQueueRouter(Options.Create(options));

        var route = router.Resolve("orders.push");

        route.Target.Profile.Should().Be(ExecutionDeliveryProfile.Pull);
        route.Target.TransportId.Should().BeNull();
        route.Target.ConsumerGroup.Should().Be("region-b");
        route.Target.ExecutionLane.Should().Be("cpu");
        route.Target.OrderingMode.Should().Be(ExecutionOrderingMode.KeyOrdered);
    }

    [Fact]
    public void Queue_policy_keys_are_trimmed_before_lookup()
    {
        var options = new QueueDeliveryOptions();
        options.Queues[" orders.push "] = new QueueDefinition
        {
            ConsumerGroup = "region-b"
        };
        var catalog = new QueueCatalog(Options.Create(options));

        var route = catalog.Resolve(" orders.push ");

        route.Queue.Should().Be("orders.push");
        route.Target.Profile.Should().Be(ExecutionDeliveryProfile.Pull);
        route.Target.ConsumerGroup.Should().Be("region-b");
        options.Queues.Should().ContainKey("orders.push");
        options.Queues.Should().NotContainKey(" orders.push ");
    }

    [Fact]
    public void Queue_definitions_reject_noncanonical_logical_queue_keys()
    {
        var options = new QueueDeliveryOptions();
        options.Queues["Orders.Push"] = new QueueDefinition();

        var action = () => options.Validate();

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*logical queue*");
    }

    [Fact]
    public void Managed_queue_policy_rejects_invalid_ordering_mode()
    {
        var options = new QueueDeliveryOptions();
        options.Queues["orders.push"] = new QueueDefinition
        {
            OrderingMode = (ExecutionOrderingMode)999
        };

        var action = () => options.Validate();

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*ordering mode*");
    }
}
