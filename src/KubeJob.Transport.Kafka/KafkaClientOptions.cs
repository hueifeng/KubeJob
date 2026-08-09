using Confluent.Kafka;

namespace KubeJob.Transport.Kafka;

internal static class KafkaClientOptions
{
    public static ProducerConfig CreateProducerConfig(KafkaBrokerNativeOptions options) => new()
    {
        BootstrapServers = options.BootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true,
        MessageTimeoutMs = 30_000,
        SecurityProtocol = KafkaBrokerNativeOptions.ParseOptionalEnum<SecurityProtocol>(options.SecurityProtocol, nameof(options.SecurityProtocol)),
        SaslMechanism = KafkaBrokerNativeOptions.ParseOptionalEnum<SaslMechanism>(options.SaslMechanism, nameof(options.SaslMechanism)),
        SaslUsername = options.SaslUsername,
        SaslPassword = options.SaslPassword
    };

    public static ConsumerConfig CreateConsumerConfig(KafkaBrokerNativeOptions options, string groupId) => new()
    {
        BootstrapServers = options.BootstrapServers,
        GroupId = groupId,
        EnableAutoCommit = false,
        EnableAutoOffsetStore = false,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        MaxPollIntervalMs = options.MaxPollIntervalMs,
        SessionTimeoutMs = options.SessionTimeoutMs,
        PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        SecurityProtocol = KafkaBrokerNativeOptions.ParseOptionalEnum<SecurityProtocol>(options.SecurityProtocol, nameof(options.SecurityProtocol)),
        SaslMechanism = KafkaBrokerNativeOptions.ParseOptionalEnum<SaslMechanism>(options.SaslMechanism, nameof(options.SaslMechanism)),
        SaslUsername = options.SaslUsername,
        SaslPassword = options.SaslPassword
    };

    public static AdminClientConfig CreateAdminConfig(KafkaBrokerNativeOptions options) => new()
    {
        BootstrapServers = options.BootstrapServers,
        SecurityProtocol = KafkaBrokerNativeOptions.ParseOptionalEnum<SecurityProtocol>(options.SecurityProtocol, nameof(options.SecurityProtocol)),
        SaslMechanism = KafkaBrokerNativeOptions.ParseOptionalEnum<SaslMechanism>(options.SaslMechanism, nameof(options.SaslMechanism)),
        SaslUsername = options.SaslUsername,
        SaslPassword = options.SaslPassword
    };
}
