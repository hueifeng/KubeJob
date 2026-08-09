namespace KubeJob.KafkaIntegrationTests;

internal static class KafkaTestEnvironment
{
    public static string GetRequiredBootstrapServers()
    {
        var bootstrapServers = Environment.GetEnvironmentVariable("KUBEJOB_KAFKA_TEST_BOOTSTRAP");
        if (string.IsNullOrWhiteSpace(bootstrapServers))
        {
            throw new InvalidOperationException("Set KUBEJOB_KAFKA_TEST_BOOTSTRAP to run Kafka integration tests.");
        }

        return bootstrapServers;
    }
}

internal sealed class KafkaFactAttribute : FactAttribute
{
    public KafkaFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KUBEJOB_KAFKA_TEST_BOOTSTRAP")))
        {
            Skip = "Set KUBEJOB_KAFKA_TEST_BOOTSTRAP to run Kafka integration tests.";
        }
    }
}
