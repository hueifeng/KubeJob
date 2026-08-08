using KubeJob.Core.Events;
using RabbitMQ.Client;

namespace KubeJob.Transport.RabbitMQ;

internal static class RabbitMqEventTopology
{
    public static string DeclareSubscription(
        IModel channel,
        RabbitMqBrokerNativeOptions options,
        string topic,
        string subscription,
        IEnumerable<EventSubscriptionDefinition> bindings)
    {
        var exchange = options.GetEventExchangeName(topic);
        var queue = options.GetEventSubscriptionQueueName(topic, subscription);
        var retryExchange = options.GetEventRetryExchangeName(topic);
        var retryQueue = options.GetEventRetryQueueName(topic, subscription);
        var deadLetterExchange = options.GetEventDeadLetterExchangeName(topic);
        var deadLetterQueue = options.GetEventDeadLetterQueueName(topic, subscription);

        channel.ExchangeDeclare(exchange, ExchangeType.Topic, durable: true, autoDelete: false);
        channel.ExchangeDeclare(retryExchange, ExchangeType.Direct, durable: true, autoDelete: false);
        channel.ExchangeDeclare(deadLetterExchange, ExchangeType.Direct, durable: true, autoDelete: false);

        channel.QueueDeclare(
            queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                ["x-dead-letter-exchange"] = deadLetterExchange,
                ["x-dead-letter-routing-key"] = subscription
            });

        foreach (var binding in bindings)
        {
            channel.QueueBind(
                queue,
                exchange,
                binding.RoutingKey);
        }

        // Retry is intentionally subscription-scoped. The TTL queue returns to
        // the subscription queue through RabbitMQ's default exchange instead
        // of republishing to the Topic; otherwise already-successful
        // subscriptions would receive the event again.
        channel.QueueDeclare(
            retryQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                ["x-message-ttl"] = checked((int)Math.Ceiling(options.RetryDelay.TotalMilliseconds)),
                ["x-dead-letter-exchange"] = string.Empty,
                ["x-dead-letter-routing-key"] = queue
            });
        channel.QueueBind(retryQueue, retryExchange, subscription);

        channel.QueueDeclare(
            deadLetterQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);
        channel.QueueBind(deadLetterQueue, deadLetterExchange, subscription);

        return queue;
    }
}
