using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.ControlPlane;

public sealed class QueueRoutingTests
{
    [Fact]
    public void Unconfigured_logical_queue_uses_the_default_definition()
    {
        var options = Options.Create(new QueueDeliveryOptions());
        var router = new ConfigurationQueueRouter(options);

        var route = router.Resolve("orders.push");

        route.Queue.Should().Be("orders.push");
        route.Target.Profile.Should().Be(ExecutionDeliveryProfile.BrokerDispatch);
        route.Target.ConsumerGroup.Should().Be("default");
        route.Target.ExecutionLane.Should().Be("default");
    }

    [Fact]
    public void Per_queue_definition_overrides_the_defaults()
    {
        var options = new QueueDeliveryOptions
        {
            Defaults =
            {
                Profile = ExecutionDeliveryProfile.BrokerDispatch,
                OrderingMode = ExecutionOrderingMode.Parallel
            }
        };
        options.Queues["orders.push"] = new QueueDefinition
        {
            Profile = ExecutionDeliveryProfile.Pull,
            ConsumerGroup = "region-b",
            OrderingMode = ExecutionOrderingMode.KeyOrdered
        };
        var router = new ConfigurationQueueRouter(Options.Create(options));

        var route = router.Resolve("orders.push");

        route.Target.Profile.Should().Be(ExecutionDeliveryProfile.Pull);
        route.Target.ConsumerGroup.Should().Be("region-b");
        route.Target.OrderingMode.Should().Be(ExecutionOrderingMode.KeyOrdered);
    }

    [Fact]
    public void Queue_policy_keys_are_trimmed_before_lookup()
    {
        var options = new QueueDeliveryOptions();
        options.Queues[" orders.push "] = new QueueDefinition
        {
            Profile = ExecutionDeliveryProfile.Pull,
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
    public void Broker_dispatch_definition_requires_a_transport_id()
    {
        var options = new QueueDeliveryOptions();
        options.Queues["orders.push"] = new QueueDefinition
        {
            Profile = ExecutionDeliveryProfile.BrokerDispatch,
            TransportId = null
        };

        var action = () => options.Validate();

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*TransportId*");
    }

    [Fact]
    public void Platform_queue_policy_can_route_a_logical_queue_to_broker_dispatch()
    {
        var options = new QueueDeliveryOptions();
        options.Queues["orders.push"] = new QueueDefinition
        {
            Profile = ExecutionDeliveryProfile.BrokerDispatch,
            TransportId = "rabbitmq"
        };
        var router = new ConfigurationQueueRouter(Options.Create(options));

        var route = router.Resolve("orders.push");

        route.Target.Profile.Should().Be(ExecutionDeliveryProfile.BrokerDispatch);
        route.Target.TransportId.Should().Be("rabbitmq");
    }

    [Fact]
    public void Execution_envelope_preserves_logical_run_identity()
    {
        var signal = new WorkAvailableSignal
        {
            SchemaVersion = WorkAvailableSignal.CurrentSchemaVersion,
            EventId = "outbox-42",
            Queue = "orders.push",
            ExecutionLane = "default",
            ConsumerGroup = "default",
            RunId = "run-42"
        };

        var envelope = ExecutionEnvelope.FromWorkAvailableSignal(signal);

        envelope.EventId.Should().Be("outbox-42");
        envelope.Queue.Should().Be("orders.push");
        envelope.RunId.Should().Be("run-42");
    }
}
