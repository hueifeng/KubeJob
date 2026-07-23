using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Transport.RabbitMQ;

public static class RabbitMqNotificationExtensions
{
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
    /// Adds queue-specific wake notifications to a remote HTTP worker. Call
    /// AddKubeJobWorkerRuntime as usual; this decorator only accelerates empty
    /// polling and does not change claim or acknowledgement semantics.
    /// </summary>
    public static IServiceCollection AddRabbitMqKubeJobWorkerNotifications(
        this IServiceCollection services,
        Action<RabbitMqNotificationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.TryAddSingleton<WorkerWakeSignal>();
        services.TryAddSingleton<HttpWorkerRuntimeClient>();
        services.Replace(ServiceDescriptor.Singleton<IWorkerRuntimeClient, NotificationAwareWorkerRuntimeClient>());
        services.AddHostedService<RabbitMqWorkerNotificationService>();
        return services;
    }
}
