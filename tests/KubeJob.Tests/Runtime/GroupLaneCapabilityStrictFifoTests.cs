using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Options;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

public sealed class GroupLaneCapabilityStrictFifoTests
{
    [Fact]
    public void Delivery_target_keeps_logical_lane_separate_from_consumer_group()
    {
        var target = new DeliveryTarget(
            ExecutionDeliveryProfile.BrokerDispatch,
            "billing-lane",
            "rabbitmq",
            "default");

        target.ExecutionLane.Should().Be("billing-lane");
        target.ConsumerGroup.Should().Be("default");

        var explicitlyGrouped = target with { ConsumerGroup = "billing-workers" };
        explicitlyGrouped.ExecutionLane.Should().Be("billing-lane");
        explicitlyGrouped.ConsumerGroup.Should().Be("billing-workers");
    }

    [Fact]
    public void Queue_catalog_resolves_lane_and_consumer_group_independently()
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
            ConsumerGroup = "group-orders"
        };

        var catalog = new QueueCatalog(Options.Create(options));

        var route = catalog.Resolve("orders.push");

        route.Target.ExecutionLane.Should().Be("lane-orders");
        route.Target.ConsumerGroup.Should().Be("group-orders");
        route.Target.ExecutionLane.Should().NotBe(route.Target.ConsumerGroup);
    }

    [Fact]
    public void Worker_profile_requires_and_normalizes_group_and_lane()
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

    [Fact]
    public void Strict_fifo_requires_single_active_consumer_and_prefetch_one()
    {
        var options = new RabbitMqExecutionOptions
        {
            PrefetchCount = 16,
            UseSingleActiveConsumer = false
        };

        var action = () => RabbitMqTopologyProvisioner.ValidateStrictFifoPolicy(
            ExecutionOrderingMode.StrictFifo,
            options);

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*StrictFifo*");
    }

    [Fact]
    public void Strict_fifo_policy_accepts_rabbitmq_safe_settings()
    {
        var options = new RabbitMqExecutionOptions
        {
            PrefetchCount = 1,
            UseSingleActiveConsumer = true
        };

        var action = () => RabbitMqTopologyProvisioner.ValidateStrictFifoPolicy(
            ExecutionOrderingMode.StrictFifo,
            options);

        action.Should().NotThrow();
    }

    [Fact]
    public void Stable_hash_matches_the_documented_fnv1a_vector()
    {
        ExecutionLaneRouter.StableHash("hello")
            .Should().Be(0x4f9f2cabU);
    }
}
