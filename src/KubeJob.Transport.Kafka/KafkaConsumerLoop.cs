using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace KubeJob.Transport.Kafka;

/// <summary>
/// Owns the Kafka client thread contract shared by Job and Event delivery:
/// one in-flight record per partition, concurrent work across partitions, and
/// acknowledgement only after the delivery action completed successfully.
/// </summary>
internal static class KafkaConsumerLoop
{
    public static async Task RunAsync(
        KafkaBrokerNativeOptions options,
        string consumerGroup,
        IReadOnlyCollection<string> sourceTopics,
        int maxConcurrency,
        Func<ConsumeResult<string, byte[]>, CancellationToken, Task> process,
        ILogger logger,
        string deliveryName,
        CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(
                KafkaClientOptions.CreateConsumerConfig(options, consumerGroup))
            .Build();
        consumer.Subscribe(sourceTopics);
        var dispatcher = new KafkaPartitionDispatcher(maxConcurrency);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await dispatcher.DrainCompletedAsync(consumer, CommitAsync, Recover);
                var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (record is null || record.IsPartitionEOF)
                {
                    continue;
                }

                if (!dispatcher.TryDispatch(record, process, stoppingToken))
                {
                    throw new InvalidOperationException($"Kafka partition '{record.TopicPartition}' was consumed while paused.");
                }

                consumer.Pause([record.TopicPartition]);
            }
        }
        finally
        {
            try { await dispatcher.DrainAsync(); } catch (OperationCanceledException) { }
            consumer.Close();
        }

        Task CommitAsync(ConsumeResult<string, byte[]> record)
        {
            consumer.Commit(record);
            return Task.CompletedTask;
        }

        void Recover(ConsumeResult<string, byte[]> record, Exception exception)
        {
            logger.LogError(exception, "Kafka {DeliveryName} transport failure at {TopicPartitionOffset}; seeking for redelivery", deliveryName, record.TopicPartitionOffset);
            consumer.Seek(record.TopicPartitionOffset);
        }
    }
}
