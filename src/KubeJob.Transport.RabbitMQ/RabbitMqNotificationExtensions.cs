using KubeJob.Core.Runtime;
using KubeJob.Core.Transport;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Transport.RabbitMQ.Telemetry;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Transport.RabbitMQ;

public static class RabbitMqNotificationExtensions
{
    /// <summary>
    /// Adds a durable RabbitMQ business-message consumer. The control plane
    /// must be registered first so the adapter can ACK only after submission.
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
    /// Adds the RabbitMQ Event subscription data plane. Each logical
    /// (Topic, Subscription) owns one queue, and all replicas of that
    /// subscription compete for deliveries. Distinct subscriptions receive
    /// independent event copies. Retry and DLQ remain subscription-scoped.
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
    /// Replaces the default polling notifier on a control-plane process.
    /// PostgreSQL remains authoritative and the transactional Outbox drives
    /// publication retries.
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
    /// Registers RabbitMQ as the legacy V2 internal execution-envelope adapter.
    /// Kept only while PostgresManaged/BrokerDispatch compatibility is retired.
    /// </summary>
    public static IServiceCollection UseRabbitMqKubeJobExecutionDispatcher(
        this IServiceCollection services,
        Action<RabbitMqExecutionOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.AddMetrics();
        services.TryAddSingleton<KubeJobRabbitMqMetrics>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IExecutionTransport, RabbitMqExecutionDispatcher>());
        return services;
    }

    /// <summary>
    /// Adds the legacy RabbitMQ Execution Envelope consumer to a Managed worker.
    /// New high-throughput queues should use AddRabbitMqKubeJobBrokerNativeConsumer.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobExecutionConsumer(
        this IServiceCollection services,
        Action<RabbitMqExecutionOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.AddMetrics();
        services.TryAddSingleton<KubeJobRabbitMqMetrics>();
        services.TryAddSingleton<QueueCatalog>();
        services.AddSingleton<RabbitMqTopologyProvisioner>();
        services.AddHostedService(services => services.GetRequiredService<RabbitMqTopologyProvisioner>());
        services.AddHostedService<RabbitMqExecutionConsumerService>();
        return services;
    }

    public static IServiceCollection UseRabbitMqKubeJobCancelPublisher(
        this IServiceCollection services,
        Action<RabbitMqExecutionOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure<RabbitMqExecutionOptions>(options => options.EnableCancelQueue = true);
        services.Configure(configure);
        services.Replace(ServiceDescriptor.Singleton<ICancelPublisher, RabbitMqCancelPublisher>());
        return services;
    }

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
