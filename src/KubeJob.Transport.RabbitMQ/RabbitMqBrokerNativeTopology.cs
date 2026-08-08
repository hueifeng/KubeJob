using RabbitMQ.Client;

namespace KubeJob.Transport.RabbitMQ;

internal static class RabbitMqBrokerNativeTopology
{
    public static void Declare(
        IModel channel,
        RabbitMqBrokerNativeOptions options,
        IReadOnlyCollection<string> logicalQueues)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logicalQueues);
        options.Validate();

        channel.ExchangeDeclare(
            exchange: options.ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);

        channel.ExchangeDeclare(
            exchange: options.GetRetryExchangeName(),
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);

        channel.ExchangeDeclare(
            exchange: options.GetDeadLetterExchangeName(),
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null);

        channel.QueueDeclare(
            queue: options.GetDeadLetterQueueName(),
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                ["x-queue-type"] = "quorum"
            });
        channel.QueueBind(
            queue: options.GetDeadLetterQueueName(),
            exchange: options.GetDeadLetterExchangeName(),
            routingKey: string.Empty,
            arguments: null);

        // One shared retry queue is enough because every retry uses the same
        // delay and RabbitMQ preserves the original logical-queue routing key
        // when dead-lettering back to the main exchange.
        channel.QueueDeclare(
            queue: options.GetRetryQueueName(),
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                ["x-queue-type"] = "quorum",
                ["x-message-ttl"] = checked((int)options.RetryDelay.TotalMilliseconds),
                ["x-dead-letter-exchange"] = options.ExchangeName
            });

        foreach (var logicalQueue in logicalQueues.Distinct(StringComparer.Ordinal))
        {
            var physicalQueue = options.GetQueueName(logicalQueue);
            channel.QueueDeclare(
                queue: physicalQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    ["x-queue-type"] = "quorum",
                    ["x-dead-letter-exchange"] = options.GetDeadLetterExchangeName()
                });
            channel.QueueBind(
                queue: physicalQueue,
                exchange: options.ExchangeName,
                routingKey: logicalQueue,
                arguments: null);

            channel.QueueBind(
                queue: options.GetRetryQueueName(),
                exchange: options.GetRetryExchangeName(),
                routingKey: logicalQueue,
                arguments: null);
        }
    }
}
