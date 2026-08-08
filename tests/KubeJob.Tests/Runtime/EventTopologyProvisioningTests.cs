using FluentAssertions;
using KubeJob.Core.Events;
using KubeJob.Core.Execution;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KubeJob.Tests.Runtime;

public sealed class EventTopologyProvisioningTests
{
    private static readonly EventKey<TestEvent> Event =
        EventKey<TestEvent>.Create("order.events", "order.created");

    [Fact]
    public void Topology_only_subscription_registration_does_not_register_a_handler()
    {
        var services = new ServiceCollection();
        services.AddKubeJobEventSubscription(Event, "audit");
        using var provider = services.BuildServiceProvider();

        provider.GetServices<EventSubscriptionDefinition>()
            .Should().ContainSingle(definition =>
                definition.Topic == "order.events"
                && definition.RoutingKey == "order.created"
                && definition.Subscription == "audit");
        provider.GetServices<IJobHandlerInvoker>().Should().BeEmpty();
    }

    [Fact]
    public void Rabbit_event_consumer_keeps_resilient_consumer_without_fail_fast_provisioner()
    {
        var services = new ServiceCollection();
        services.AddRabbitMqKubeJobEventConsumer(options =>
            options.ConnectionString = "amqp://guest:guest@localhost:5672/");

        var hosted = services
            .Where(descriptor => descriptor.ServiceType == typeof(IHostedService))
            .Select(descriptor => descriptor.ImplementationType)
            .Where(type => type is not null)
            .ToArray();

        hosted.Should().Contain(typeof(RabbitMqBrokerNativeEventConsumerService));
        hosted.Should().NotContain(typeof(RabbitMqEventTopologyProvisionerService));
    }

    [Fact]
    public void Standalone_event_topology_provisioner_does_not_register_consumer()
    {
        var services = new ServiceCollection();
        services.AddRabbitMqKubeJobEventTopologyProvisioner(options =>
            options.ConnectionString = "amqp://guest:guest@localhost:5672/");

        var hosted = services
            .Where(descriptor => descriptor.ServiceType == typeof(IHostedService))
            .Select(descriptor => descriptor.ImplementationType)
            .Where(type => type is not null)
            .ToArray();

        hosted.Should().Contain(typeof(RabbitMqEventTopologyProvisionerService));
        hosted.Should().NotContain(typeof(RabbitMqBrokerNativeEventConsumerService));
    }

    private sealed record TestEvent(string Id);
}
