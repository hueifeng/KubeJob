using KubeJob.Core.Events;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Declares durable RabbitMQ Event Topic/Subscription topology before consumers
/// start. This can also run in a topology-only deployment role so durable
/// subscription queues exist while handler workers are offline.
/// </summary>
public sealed class RabbitMqEventTopologyProvisionerService : IHostedService
{
    private readonly RabbitMqBrokerNativeOptions _options;
    private readonly EventSubscriptionDefinition[] _subscriptions;
    private readonly ILogger<RabbitMqEventTopologyProvisionerService> _logger;

    public RabbitMqEventTopologyProvisionerService(
        IOptions<RabbitMqBrokerNativeOptions> options,
        IEnumerable<EventSubscriptionDefinition> subscriptions,
        ILogger<RabbitMqEventTopologyProvisionerService> logger)
    {
        _options = options.Value;
        _subscriptions = subscriptions.ToArray();
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _options.Validate();
        if (_subscriptions.Length == 0)
        {
            throw new InvalidOperationException(
                "RabbitMQ Event topology provisioning requires at least one registered EventSubscriptionDefinition. " +
                "Register AddKubeJobEventHandler or AddKubeJobEventSubscription first.");
        }

        var groups = _subscriptions
            .GroupBy(item => (item.Topic, item.Subscription))
            .ToArray();

        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection("KubeJob.Events.TopologyProvisioner");
        using var channel = connection.CreateModel();

        foreach (var group in groups)
        {
            var bindings = ValidateBindings(
                group.Key.Topic,
                group.Key.Subscription,
                group);
            RabbitMqEventTopology.DeclareSubscription(
                channel,
                _options,
                group.Key.Topic,
                group.Key.Subscription,
                bindings);
        }

        _logger.LogInformation(
            "Provisioned {SubscriptionCount} RabbitMQ KubeJob Event subscription queue(s)",
            groups.Length);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static EventSubscriptionDefinition[] ValidateBindings(
        string topic,
        string subscription,
        IEnumerable<EventSubscriptionDefinition> definitions)
    {
        var byRoutingKey = new Dictionary<string, EventSubscriptionDefinition>(StringComparer.Ordinal);
        foreach (var definition in definitions)
        {
            if (!byRoutingKey.TryAdd(definition.RoutingKey, definition))
            {
                throw new InvalidOperationException(
                    $"Subscription '{topic}/{subscription}' has multiple registrations for routing key " +
                    $"'{definition.RoutingKey}'. Register each routing key once per Subscription.");
            }
        }

        return byRoutingKey.Values.ToArray();
    }
}
