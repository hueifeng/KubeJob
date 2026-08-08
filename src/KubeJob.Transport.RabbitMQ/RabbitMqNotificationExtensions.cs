using KubeJob.Core.Runtime;
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
    /// Adds the RabbitMQ-authoritative BrokerNative data plane. Pair this with
    /// AddKubeJobBrokerNativeWorker, not AddKubeJobWorker: no control-plane
    /// runtime client or Managed Claim/Lease loop is required for consumption.
    /// One physical execution queue is declared per logical worker queue and
    /// all replicas configured for that queue compete for its deliveries.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobBrokerNativeConsumer(
        this IServiceCollection services,
        Action<RabbitMqBrokerNativeOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.AddHostedService<RabbitMqBrokerNativeConsumerService>();
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
    /// Registers RabbitMQ as the internal execution-envelope adapter. A Queue
    /// must be routed to BrokerDispatch separately; this method does not alter
    /// the business submission contract.
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
    /// Adds the RabbitMQ Execution Envelope consumer to a worker. The worker
    /// must already be registered with AddKubeJobWorker. The Direct Dispatch
    /// topology (group exchange, per-logical-queue TTL retry and quorum
    /// execution queues, group DLX/DLQ, and the optional per-group cancel fanout
    /// exchange) is declared automatically on startup.
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

    /// <summary>
    /// Replaces the default no-op <c>ICancelPublisher</c> with a RabbitMQ
    /// publisher that fans out per-group cancel signals to all workers in
    /// the consumer group. Pair with <c>UseRabbitMqKubeJobExecutionDispatcher</c>
    /// when enabling Direct Dispatch Mode.
    /// </summary>
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

    /// <summary>
    /// Adds queue-specific wake notifications to a remote HTTP worker. Call
    /// AddKubeJobWorker first. The listener pulses the worker's claim trigger
    /// and does not change claim, lease, or completion semantics.
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
