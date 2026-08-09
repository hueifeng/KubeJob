namespace KubeJob.RabbitMqIntegrationTests;

internal static class RabbitMqTestEnvironment
{
    public static string GetRequiredConnectionString()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            "KUBEJOB_RABBITMQ_TEST_CONNECTION");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "Set KUBEJOB_RABBITMQ_TEST_CONNECTION to run RabbitMQ integration tests.");
        }

        return connectionString;
    }
}

/// <summary>
/// Skips RabbitMQ integration tests during discovery when a broker has not
/// been configured. A discovery-time skip works with the xUnit runner version
/// used by this repository; throwing a dynamic skip from an async test does
/// not.
/// </summary>
internal sealed class RabbitMqFactAttribute : FactAttribute
{
    public RabbitMqFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(
                Environment.GetEnvironmentVariable("KUBEJOB_RABBITMQ_TEST_CONNECTION")))
        {
            Skip = "Set KUBEJOB_RABBITMQ_TEST_CONNECTION to run RabbitMQ integration tests.";
        }
    }
}
