using System.Text;

namespace KubeJob.Transport.RabbitMQ;

public sealed class RabbitMqJobIngressOptions
{
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    public string ExchangeName { get; set; } = "kubejob.job-ingress";

    public string QueueName { get; set; } = "kubejob.job-ingress.queue";

    public string RoutingKey { get; set; } = "#";

    public string Source { get; set; } = "rabbitmq";

    public string? DeadLetterExchangeName { get; set; }

    public string? DeadLetterRoutingKey { get; set; }

    /// <summary>
    /// Allow the ingress queue to be declared without a dead-letter exchange.
    /// When false (default), <see cref="Validate"/> throws because permanent
    /// rejects (malformed JSON, validation errors, idempotency conflicts) would
    /// otherwise be silently dropped by the broker.
    /// </summary>
    public bool AllowNoDeadLetterExchange { get; set; }

    /// <summary>
    /// Maximum unacknowledged ingress deliveries. Keep this at least as large
    /// as <see cref="SubmissionBatchSize"/> when micro-batching is enabled.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 100;

    /// <summary>
    /// Concurrent RabbitMQ delivery callbacks. Zero derives a value from the
    /// prefetch/batch configuration so the micro-batcher can actually receive
    /// a full batch instead of being serialized by the client default of one.
    /// </summary>
    public ushort ConsumerDispatchConcurrency { get; set; }

    /// <summary>
    /// Maximum number of broker messages committed in one durable submission
    /// transaction. A smaller batch is flushed when <see cref="SubmissionBatchWait"/>
    /// elapses, so low traffic never waits for this count.
    /// </summary>
    public int SubmissionBatchSize { get; set; } = 100;

    /// <summary>
    /// Maximum time the first message in a submission micro-batch waits before
    /// it is durably submitted. Set this low for latency-sensitive queues.
    /// </summary>
    public TimeSpan SubmissionBatchWait { get; set; } = TimeSpan.FromMilliseconds(10);

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
        ArgumentException.ThrowIfNullOrWhiteSpace(QueueName);
        ArgumentException.ThrowIfNullOrWhiteSpace(RoutingKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(Source);
        if (!string.IsNullOrWhiteSpace(DeadLetterRoutingKey)
            && string.IsNullOrWhiteSpace(DeadLetterExchangeName))
        {
            throw new InvalidOperationException(
                "DeadLetterRoutingKey requires DeadLetterExchangeName.");
        }

        if (string.IsNullOrWhiteSpace(DeadLetterExchangeName) && !AllowNoDeadLetterExchange)
        {
            throw new InvalidOperationException(
                "DeadLetterExchangeName is required for the RabbitMQ business ingress. " +
                "Permanent rejects (malformed JSON, validation errors, idempotency conflicts) " +
                "must be routed to a dead-letter exchange so they are not silently dropped. " +
                "Set DeadLetterExchangeName and DeadLetterRoutingKey, or opt out explicitly with " +
                "AllowNoDeadLetterExchange=true.");
        }

        if (!string.IsNullOrWhiteSpace(DeadLetterExchangeName)
            && Encoding.UTF8.GetByteCount(DeadLetterExchangeName) >= 255)
        {
            throw new InvalidOperationException(
                "DeadLetterExchangeName must be shorter than 255 UTF-8 bytes.");
        }

        if (Encoding.UTF8.GetByteCount(ExchangeName) >= 255
            || Encoding.UTF8.GetByteCount(QueueName) >= 255)
        {
            throw new InvalidOperationException(
                "RabbitMQ ExchangeName and QueueName must be shorter than 255 UTF-8 bytes.");
        }

        if (Encoding.UTF8.GetByteCount(RoutingKey) >= 255)
        {
            throw new InvalidOperationException(
                "RabbitMQ RoutingKey must be shorter than 255 UTF-8 bytes.");
        }

        if (ReconnectDelay <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("ReconnectDelay must be positive.");
        }

        if (PrefetchCount == 0)
        {
            throw new InvalidOperationException("PrefetchCount must be positive.");
        }

        if (SubmissionBatchSize is < 1 or > 10_000)
        {
            throw new InvalidOperationException("SubmissionBatchSize must be between 1 and 10000.");
        }

        if (SubmissionBatchWait <= TimeSpan.Zero || SubmissionBatchWait > TimeSpan.FromSeconds(1))
        {
            throw new InvalidOperationException("SubmissionBatchWait must be positive and no greater than one second.");
        }

        if (PrefetchCount < SubmissionBatchSize)
        {
            throw new InvalidOperationException(
                "PrefetchCount must be at least SubmissionBatchSize so a full micro-batch can be received.");
        }

        if (ConsumerDispatchConcurrency > PrefetchCount)
        {
            throw new InvalidOperationException(
                "ConsumerDispatchConcurrency cannot exceed PrefetchCount.");
        }
    }
}
