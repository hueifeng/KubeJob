namespace KubeJob.Transport.RabbitMQ;

public sealed class RabbitMqNotificationOptions
{
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    public string ExchangeName { get; set; } = "kubejob.work-available";

    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    public void Validate()
    {
        if (!Uri.TryCreate(ConnectionString, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("amqp" or "amqps"))
        {
            throw new InvalidOperationException(
                "RabbitMQ ConnectionString must be an absolute amqp or amqps URI.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(ExchangeName);
        if (ReconnectDelay <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("ReconnectDelay must be positive.");
        }
    }
}
