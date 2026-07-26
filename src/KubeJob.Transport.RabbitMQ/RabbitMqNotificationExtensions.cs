using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
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
        services.Replace(ServiceDescriptor.Singleton<IExecutionGroupResolver, RabbitMqExecutionGroupResolver>());
        services.Replace(ServiceDescriptor.Singleton<IExecutionDispatcher, RabbitMqExecutionDispatcher>());
        return services;
    }

    /// <summary>
    /// Adds the RabbitMQ Execution Envelope consumer to a worker. The worker
    /// must already be registered with AddKubeJobWorker. The Direct Dispatch
    /// topology (group exchange, TTL retry exchange/queues, DLX, DLQ,
    /// per-queue quorum queues with optional <c>x-delivery-limit</c>, and the
    /// per-group cancel fanout exchange) is declared automatically on startup.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobExecutionConsumer(
        this IServiceCollection services,
        Action<RabbitMqExecutionOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.AddHostedService<RabbitMqDispatchTopology>();
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
        services.Configure(configure);
        services.Replace(ServiceDescriptor.Singleton<IExecutionGroupResolver, RabbitMqExecutionGroupResolver>());
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
