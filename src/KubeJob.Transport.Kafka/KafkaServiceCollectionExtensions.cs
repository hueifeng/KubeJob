using KubeJob.Core.Transport;
using KubeJob.Core.Events;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Transport.Kafka;

public static class KafkaServiceCollectionExtensions
{
    /// <summary>Registers Kafka as a producer-only BrokerNative transport.</summary>
    public static IServiceCollection AddKafkaKubeJobBrokerNativeTransport(
        this IServiceCollection services,
        Action<KafkaBrokerNativeOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IMessageTransportPublisher, KafkaBrokerNativePublisher>());
        return services;
    }

    /// <summary>
    /// Adds Kafka-authoritative BrokerNative Job delivery. Every replica using
    /// the same deployment environment shares the jobs consumer group.
    /// </summary>
    public static IServiceCollection AddKafkaKubeJobBrokerNativeConsumer(
        this IServiceCollection services,
        Action<KafkaBrokerNativeOptions> configure)
    {
        services.AddKafkaKubeJobBrokerNativeTransport(configure);
        services.AddHostedService<KafkaBrokerNativeConsumerService>();
        return services;
    }

    /// <summary>
    /// Adds the Kafka Event Runtime. Handlers must use subscription names log,
    /// data or notify; replicas within one capability group scale horizontally.
    /// </summary>
    public static IServiceCollection AddKafkaKubeJobEventConsumer(
        this IServiceCollection services,
        Action<KafkaBrokerNativeOptions> configure)
    {
        services.AddKafkaKubeJobBrokerNativeTransport(configure);
        services.TryAddSingleton<IEventInboxStore, MissingEventInboxStore>();
        services.AddHostedService<KafkaBrokerNativeEventConsumerService>();
        return services;
    }
}
