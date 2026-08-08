using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Worker.Options;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

public sealed class GroupLaneCapabilityStrictFifoTests
{
    [Fact]
    public void Managed_delivery_target_keeps_worker_lane_separate_from_consumer_group()
    {
        var target = new DeliveryTarget(
            ExecutionDeliveryProfile.Pull,
            "billing-lane",
            null,
            "default");

        target.ExecutionLane.Should().Be("billing-lane");
        target.ConsumerGroup.Should().Be("default");
        target.TransportId.Should().BeNull();

        var explicitlyGrouped = target with { ConsumerGroup = "billing-workers" };
        explicitlyGrouped.ExecutionLane.Should().Be("billing-lane");
        explicitlyGrouped.ConsumerGroup.Should().Be("billing-workers");
    }

    [Fact]
    public void Queue_catalog_resolves_managed_lane_and_consumer_group_independently()
    {
        var options = new QueueDeliveryOptions
        {
            Defaults =
            {
                ExecutionLane = "lane-default",
                ConsumerGroup = "group-default"
            }
        };
        options.Queues["orders.push"] = new QueueDefinition
        {
            ExecutionLane = "lane-orders",
            ConsumerGroup = "group-orders",
            OrderingMode = ExecutionOrderingMode.StrictFifo
        };

        var catalog = new QueueCatalog(Options.Create(options));

        var route = catalog.Resolve("orders.push");

        route.Target.Profile.Should().Be(ExecutionDeliveryProfile.Pull);
        route.Target.ExecutionLane.Should().Be("lane-orders");
        route.Target.ConsumerGroup.Should().Be("group-orders");
        route.Target.OrderingMode.Should().Be(ExecutionOrderingMode.StrictFifo);
        route.Target.ExecutionLane.Should().NotBe(route.Target.ConsumerGroup);
    }

    [Fact]
    public void Managed_worker_profile_requires_and_normalizes_group_and_lane()
    {
        var options = new KubeJobWorkerOptions
        {
            ConsumerGroup = "  group-orders ",
            ExecutionLane = "  lane-orders ",
            Queues = new List<string> { "orders.push" }
        };

        options.Validate();

        options.ConsumerGroup.Should().Be("group-orders");
        options.ExecutionLane.Should().Be("lane-orders");
    }
}
