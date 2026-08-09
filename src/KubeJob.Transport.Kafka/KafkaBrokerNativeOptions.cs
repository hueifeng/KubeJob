using System.Text;
using KubeJob.Core.Queues;
using KubeJob.Core.Runtime;

namespace KubeJob.Transport.Kafka;

/// <summary>
/// Kafka data-plane options for BrokerNative jobs and the fixed log, data and
/// notify event capabilities. Topic creation is deliberately opt-in: a
/// production deployment should provision and validate topics separately.
/// </summary>
public sealed class KafkaBrokerNativeOptions
{
    private static readonly TimeSpan[] SupportedRetryDelays =
    [
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(30),
        TimeSpan.FromMinutes(5),
        TimeSpan.FromMinutes(30)
    ];

    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Deployment namespace used for Kafka consumer group names.</summary>
    public string Environment { get; set; } = "default";

    /// <summary>Shared business-event topic, equivalent to order.exchange.</summary>
    public string EventTopic { get; set; } = "order.events";

    /// <summary>Prefix for one Kafka topic per logical BrokerNative job queue.</summary>
    public string JobTopicPrefix { get; set; } = "kubejob.jobs";

    public string ConsumerGroupPrefix { get; set; } = "kubejob";

    public int MaxPollIntervalMs { get; set; } = 300_000;

    public int SessionTimeoutMs { get; set; } = 45_000;

    public int ReconnectDelayMilliseconds { get; set; } = 2_000;

    public bool CreateTopicsOnStartup { get; set; }

    public int TopicPartitions { get; set; } = 6;

    public short ReplicationFactor { get; set; } = 3;

    public string? SaslMechanism { get; set; }

    public string? SecurityProtocol { get; set; }

    public string? SaslUsername { get; set; }

    public string? SaslPassword { get; set; }

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(BootstrapServers);
        ValidateName(Environment, nameof(Environment));
        ValidateName(EventTopic, nameof(EventTopic));
        ValidateName(JobTopicPrefix, nameof(JobTopicPrefix));
        ValidateName(ConsumerGroupPrefix, nameof(ConsumerGroupPrefix));

        if (MaxPollIntervalMs is < 1_000 or > 3_600_000)
        {
            throw new InvalidOperationException("Kafka MaxPollIntervalMs must be between 1 second and 1 hour.");
        }

        if (SessionTimeoutMs is < 6_000 or > 300_000)
        {
            throw new InvalidOperationException("Kafka SessionTimeoutMs must be between 6 seconds and 5 minutes.");
        }

        if (ReconnectDelayMilliseconds <= 0)
        {
            throw new InvalidOperationException("Kafka ReconnectDelayMilliseconds must be positive.");
        }

        if (CreateTopicsOnStartup && (TopicPartitions <= 0 || ReplicationFactor <= 0))
        {
            throw new InvalidOperationException("Kafka topic partition and replication counts must be positive.");
        }

        var securityProtocol = ParseOptionalEnum<Confluent.Kafka.SecurityProtocol>(SecurityProtocol, nameof(SecurityProtocol));
        var saslMechanism = ParseOptionalEnum<Confluent.Kafka.SaslMechanism>(SaslMechanism, nameof(SaslMechanism));
        if (saslMechanism is not null && securityProtocol is not (Confluent.Kafka.SecurityProtocol.SaslPlaintext or Confluent.Kafka.SecurityProtocol.SaslSsl))
        {
            throw new InvalidOperationException("Kafka SaslMechanism requires SecurityProtocol to be SaslPlaintext or SaslSsl.");
        }

        if (securityProtocol is Confluent.Kafka.SecurityProtocol.SaslPlaintext or Confluent.Kafka.SecurityProtocol.SaslSsl)
        {
            if (saslMechanism is null
                || string.IsNullOrWhiteSpace(SaslUsername)
                || string.IsNullOrWhiteSpace(SaslPassword))
            {
                throw new InvalidOperationException(
                    "Kafka SASL security requires SaslMechanism, SaslUsername, and SaslPassword.");
            }
        }
        else if (!string.IsNullOrWhiteSpace(SaslUsername) || !string.IsNullOrWhiteSpace(SaslPassword))
        {
            throw new InvalidOperationException(
                "Kafka SaslUsername and SaslPassword require a SASL SecurityProtocol.");
        }
    }

    public string GetJobTopic(string logicalQueue)
        => ValidateName($"{JobTopicPrefix}.{LogicalQueueName.Normalize(logicalQueue, nameof(logicalQueue))}", "job topic");

    public string GetJobRetryTopic(string logicalQueue) => $"{GetJobTopic(logicalQueue)}.retry";

    public string GetJobDeadLetterTopic(string logicalQueue) => $"{GetJobTopic(logicalQueue)}.dlq";

    public string GetEventConsumerGroup(string capability)
        => $"{ConsumerGroupPrefix}.{Environment}.{GetCapability(capability)}";

    public string GetEventRetryTopic(string capability)
        => $"{EventTopic}.{GetCapability(capability)}.retry";

    public string GetEventDeadLetterTopic(string capability)
        => $"{EventTopic}.{GetCapability(capability)}.dlq";

    public string GetJobConsumerGroup() => $"{ConsumerGroupPrefix}.{Environment}.jobs";

    /// <summary>
    /// Kafka has no broker-native per-record TTL. The adapter rounds a policy
    /// delay up to one visible, bounded retry tier rather than pretending to
    /// support arbitrary delayed delivery.
    /// </summary>
    public TimeSpan GetRetryDelay(RetryPolicy? policy, int failedAttempt)
    {
        var requested = policy?.ComputeDelay(Math.Max(1, failedAttempt))
            ?? SupportedRetryDelays[0];
        return SupportedRetryDelays.FirstOrDefault(delay => delay >= requested, SupportedRetryDelays[^1]);
    }

    internal static string GetCapability(string capability)
    {
        var value = LogicalQueueName.Normalize(capability, nameof(capability));
        return value is "log" or "data" or "notify"
            ? value
            : throw new InvalidOperationException(
                "Kafka event subscriptions must target one fixed capability: log, data, or notify.");
    }

    private static string ValidateName(string value, string kind)
    {
        if (Encoding.UTF8.GetByteCount(value) > 249)
        {
            throw new InvalidOperationException($"Kafka {kind} must not exceed 249 UTF-8 bytes.");
        }

        return value;
    }

    internal static TEnum? ParseOptionalEnum<TEnum>(string? value, string optionName)
        where TEnum : struct, Enum
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        if (!Enum.TryParse<TEnum>(value, ignoreCase: true, out var parsed))
        {
            throw new InvalidOperationException($"Kafka {optionName} value '{value}' is invalid.");
        }

        return parsed;
    }
}
