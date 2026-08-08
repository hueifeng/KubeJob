using KubeJob.Core.Transport;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Transport.RabbitMQ;

public static class RabbitMqNotificationExtensions
{
    /// <summary>
    /// Adds a durable RabbitMQ business-message ingress consumer. This is an
    /// optional integration surface and is separate from KubeJob execution
    /// delivery authority.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobIngress(
        this IServiceCollection services,
        Action<RabbitMqJobIngressOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.AddHostedService<RabbitMqJobIngressService>();
        return services;
    }

    /// <summary>
    /// Registers RabbitMQ as a transport-neutral BrokerNative publisher for
    /// both Job Queue and Event Topic messages. This registration is
    /// producer-only and does not start a Worker.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobBrokerNativeTransport(
        this IServiceCollection services,
        Action<RabbitMqBrokerNativeOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IMessageTransportPublisher, RabbitMqBrokerNativePublisher>());
        return services;
    }

    /// <summary>
    /// Adds the RabbitMQ-authoritative BrokerNative Job data plane. Pair this
    /// with AddKubeJobBrokerNativeWorker. One physical execution queue is
    /// declared per logical Job Queue and all worker replicas compete for it.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobBrokerNativeConsumer(
        this IServiceCollection services,
        Action<RabbitMqBrokerNativeOptions> configure)
    {
        services.AddRabbitMqKubeJobBrokerNativeTransport(configure);
        services.AddHostedService<RabbitMqBrokerNativeConsumerService>();
        return services;
    }

    /// <summary>
    /// Declares durable Event Topic/Subscription queues from the registered
    /// EventSubscriptionDefinitions without starting a consumer. Use this in a
    /// deployment/migration step when subscriptions must exist before handler
    /// workers come online so events can accumulate durably while workers are
    /// offline. Provisioning is intentionally fail-fast so a deployment step
    /// cannot report success while the requested durable topology is missing.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobEventTopologyProvisioner(
        this IServiceCollection services,
        Action<RabbitMqBrokerNativeOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.AddHostedService<RabbitMqEventTopologyProvisionerService>();
        return services;
    }

    /// <summary>
    /// Adds the RabbitMQ Event subscription data plane. Each logical
    /// (Topic, Subscription) owns one durable queue, and all replicas of that
    /// subscription compete for deliveries. Distinct subscriptions receive
    /// independent event copies. Retry and DLQ remain subscription-scoped.
    /// The consumer declares its own topology immediately before BasicConsume
    /// and keeps its reconnect loop, so a temporary broker outage does not turn
    /// normal worker startup into a fail-fast provisioning dependency.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobEventConsumer(
        this IServiceCollection services,
        Action<RabbitMqBrokerNativeOptions> configure)
    {
        services.AddRabbitMqKubeJobBrokerNativeTransport(configure);
        services.AddHostedService<RabbitMqBrokerNativeEventConsumerService>();
        return services;
    }

    /// <summary>
    /// Optional wake notification for PostgresManaged workers. PostgreSQL
    /// remains the queue authority; losing RabbitMQ only falls back to polling.
    /// </summary>
    public static IServiceCollection UseRabbitMqKubeJobNotifications(
        this IServiceCollection services,
        Action<RabbitMqNotificationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.Replace(ServiceDescriptor.Singleton<IWorkAvailableNotifier, RabbitMqWorkAvailableNotifier>());
        return services;
    }

    /// <summary>
    /// Optional wake notification consumer for PostgresManaged workers. This
    /// never carries execution ownership and is independent of BrokerNative.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobWorkerNotifications(
        this IServiceCollection services,
        Action<RabbitMqNotificationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.AddKubeJobWorkerClaimTrigger();
        services.AddHostedService<RabbitMqWorkerNotificationService>();
        return services;
    }
}
