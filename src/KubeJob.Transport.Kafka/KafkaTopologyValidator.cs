using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KubeJob.Transport.Kafka;

/// <summary>Validates provisioned topics and creates them only for local development.</summary>
internal static class KafkaTopologyValidator
{
    public static async Task EnsureAsync(
        KafkaBrokerNativeOptions options,
        IEnumerable<string> topics,
        CancellationToken cancellationToken)
    {
        var expected = topics.Distinct(StringComparer.Ordinal).ToArray();
        if (expected.Length == 0)
        {
            return;
        }

        using var admin = new AdminClientBuilder(KafkaClientOptions.CreateAdminConfig(options)).Build();
        var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
        var existing = metadata.Topics
            .Where(topic => topic.Error.Code == ErrorCode.NoError)
            .Select(topic => topic.Topic)
            .ToHashSet(StringComparer.Ordinal);
        var missing = expected.Where(topic => !existing.Contains(topic)).ToArray();
        if (missing.Length == 0)
        {
            return;
        }

        if (!options.CreateTopicsOnStartup)
        {
            throw new InvalidOperationException(
                $"Kafka topics are missing: {string.Join(", ", missing)}. " +
                "Provision them before startup, or set CreateTopicsOnStartup only for local development.");
        }

        try
        {
            await admin.CreateTopicsAsync(
                missing.Select(topic => new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = options.TopicPartitions,
                    ReplicationFactor = options.ReplicationFactor
                }),
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });
        }
        catch (CreateTopicsException exception) when (
            exception.Results.All(result => result.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
        {
            // Another replica won the startup race; the next connection will
            // validate the topic normally.
        }

        cancellationToken.ThrowIfCancellationRequested();
    }
}
