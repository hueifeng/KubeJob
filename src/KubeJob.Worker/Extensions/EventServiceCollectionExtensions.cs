using KubeJob.Core.Events;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Worker.Extensions;

public static class EventServiceCollectionExtensions
{
    /// <summary>
    /// Registers only the logical Event Subscription definition. This is useful
    /// for deployment-time broker topology provisioning before any handler
    /// worker starts consuming. It does not register a handler or consumer by
    /// itself.
    /// </summary>
    public static IServiceCollection AddKubeJobEventSubscription<TEvent>(
        this IServiceCollection services,
        EventKey<TEvent> eventKey,
        string subscription)
    {
        services.AddSingleton(CreateDefinition(eventKey, subscription));
        return services;
    }

    /// <summary>
    /// Registers one independent event Subscription and its typed handler. All
    /// worker replicas that use the same subscription name compete for the same
    /// broker delivery stream; a different subscription name receives its own
    /// copy.
    /// </summary>
    public static IServiceCollection AddKubeJobEventHandler<TEvent, THandler>(
        this IServiceCollection services,
        EventKey<TEvent> eventKey,
        string subscription)
        where THandler : class, IKubeEventHandler<TEvent>
    {
        var definition = CreateDefinition(eventKey, subscription);

        services.AddScoped<THandler>();
        services.AddSingleton(definition);
        services.AddSingleton<IJobHandlerInvoker>(
            new EventHandlerInvoker<THandler, TEvent>(definition));
        services.TryAddSingleton<JobHandlerRegistry>();
        services.TryAddSingleton<BrokerNativeEventProcessor>();
        return services;
    }

    private static EventSubscriptionDefinition CreateDefinition<TEvent>(
        EventKey<TEvent> eventKey,
        string subscription)
    {
        if (eventKey.IsEmpty)
        {
            throw new ArgumentException("The event key must be initialized.", nameof(eventKey));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        var normalizedSubscription = subscription.Trim();
        return new EventSubscriptionDefinition(
            eventKey.Topic.Trim(),
            eventKey.RoutingKey.Trim(),
            normalizedSubscription,
            EventSubscriptionDefinition.CreateHandlerKey(
                eventKey.Topic,
                eventKey.RoutingKey,
                normalizedSubscription));
    }
}
