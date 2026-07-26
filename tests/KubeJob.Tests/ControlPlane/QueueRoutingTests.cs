using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.ControlPlane;

public sealed class QueueRoutingTests
{
    [Fact]
    public void Unconfigured_logical_queue_uses_pull_profile()
    {
        var router = new ConfigurationQueueRouter(
            Options.Create(new QueueDeliveryOptions()));

        var route = router.Resolve("orders.push");

        route.Queue.Should().Be("orders.push");
        route.Profile.Should().Be(ExecutionDeliveryProfile.Pull);
    }

    [Fact]
    public void Platform_queue_policy_can_route_a_logical_queue_to_broker_dispatch()
    {
        var options = new QueueDeliveryOptions();
        options.QueueProfiles["orders.push"] = ExecutionDeliveryProfile.BrokerDispatch;
        var router = new ConfigurationQueueRouter(Options.Create(options));

        var route = router.Resolve("orders.push");

        route.Profile.Should().Be(ExecutionDeliveryProfile.BrokerDispatch);
    }

    [Fact]
    public void Execution_envelope_preserves_logical_run_identity()
    {
        var signal = new WorkAvailableSignal(
            WorkAvailableSignal.CurrentSchemaVersion,
            "outbox-42",
            "orders.push",
            "run-42");

        var envelope = ExecutionEnvelope.FromWorkAvailableSignal(signal);

        envelope.EventId.Should().Be("outbox-42");
        envelope.Queue.Should().Be("orders.push");
        envelope.RunId.Should().Be("run-42");
    }
}
