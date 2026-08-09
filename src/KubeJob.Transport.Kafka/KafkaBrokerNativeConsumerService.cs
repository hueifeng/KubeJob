using System.Text.Json;
using Confluent.Kafka;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Transport.Kafka;

/// <summary>
/// Kafka-authoritative BrokerNative Job consumer. A job queue maps to one
/// topic; replicas with the same group id distribute its partitions.
/// </summary>
public sealed class KafkaBrokerNativeConsumerService : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly KafkaBrokerNativeOptions _options;
    private readonly KubeJobWorkerOptions _worker;
    private readonly BrokerNativeJobProcessor _processor;
    private readonly ILogger<KafkaBrokerNativeConsumerService> _logger;
    private readonly IProducer<string, byte[]> _retryProducer;

    public KafkaBrokerNativeConsumerService(
        IOptions<KafkaBrokerNativeOptions> options,
        IOptions<KubeJobWorkerOptions> worker,
        BrokerNativeJobProcessor processor,
        ILogger<KafkaBrokerNativeConsumerService> logger)
    {
        _options = options.Value;
        _worker = worker.Value;
        _processor = processor;
        _logger = logger;
        _options.Validate();
        _worker.Validate();
        _retryProducer = new ProducerBuilder<string, byte[]>(KafkaClientOptions.CreateProducerConfig(_options)).Build();
    }

    public override void Dispose()
    {
        _retryProducer.Dispose();
        base.Dispose();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topics = _worker.Queues.SelectMany(queue => new[]
        {
            _options.GetJobTopic(queue),
            _options.GetJobRetryTopic(queue),
            _options.GetJobDeadLetterTopic(queue)
        }).ToArray();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await KafkaTopologyValidator.EnsureAsync(_options, topics, stoppingToken);
                await ConsumeAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Kafka BrokerNative consumer disconnected for worker {WorkerId}; reconnecting", _worker.WorkerId);
                await Task.Delay(_options.ReconnectDelayMilliseconds, stoppingToken);
            }
        }
    }

    private async Task ConsumeAsync(CancellationToken stoppingToken)
    {
        var sources = _worker.Queues.SelectMany(queue => new[]
        {
            _options.GetJobTopic(queue),
            _options.GetJobRetryTopic(queue)
        }).ToArray();
        await KafkaConsumerLoop.RunAsync(
            _options,
            _options.GetJobConsumerGroup(),
            sources,
            _worker.MaxConcurrentJobs,
            ProcessAsync,
            _logger,
            "Job",
            stoppingToken);
    }

    private async Task ProcessAsync(ConsumeResult<string, byte[]> record, CancellationToken stoppingToken)
    {
        await WaitForRetryDueAsync(record.Message.Headers, stoppingToken);
        var isRetry = record.Topic.EndsWith(".retry", StringComparison.Ordinal);
        var queue = _worker.Queues.SingleOrDefault(candidate =>
            string.Equals(record.Topic, isRetry ? _options.GetJobRetryTopic(candidate) : _options.GetJobTopic(candidate), StringComparison.Ordinal));
        if (queue is null)
        {
            await PublishAsync(record, _options.GetJobDeadLetterTopic(_worker.Queues[0]), stoppingToken);
            return;
        }

        BrokerNativeJobMessage message;
        try
        {
            message = JsonSerializer.Deserialize<BrokerNativeJobMessage>(record.Message.Value, SerializerOptions)
                ?? throw new JsonException("BrokerNative job message was empty.");
            message = message with { RetryPolicy = message.RetryPolicy ?? new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5)) };
            message.Validate();
            if (!string.Equals(message.Queue, queue, StringComparison.Ordinal))
            {
                throw new JsonException($"BrokerNative message queue '{message.Queue}' does not match topic '{record.Topic}'.");
            }
        }
        catch (Exception exception) when (exception is JsonException or InvalidOperationException or ArgumentException)
        {
            _logger.LogWarning(exception, "Kafka Job delivery {TopicPartitionOffset} is malformed; sending to DLQ", record.TopicPartitionOffset);
            await PublishAsync(record, _options.GetJobDeadLetterTopic(queue), stoppingToken);
            return;
        }

        var result = await _processor.ProcessAsync(message, CancellationToken.None, stoppingToken);
        if (result.Disposition == BrokerNativeMessageDisposition.Ack)
        {
            return;
        }

        if (result.Disposition == BrokerNativeMessageDisposition.Retry && result.RetryMessage is not null)
        {
            await PublishAsync(
                record,
                _options.GetJobRetryTopic(queue),
                stoppingToken,
                JsonSerializer.SerializeToUtf8Bytes(result.RetryMessage, SerializerOptions),
                DateTimeOffset.UtcNow + _options.GetRetryDelay(result.RetryMessage.RetryPolicy, message.Attempt));
            return;
        }

        await PublishAsync(record, _options.GetJobDeadLetterTopic(queue), stoppingToken);
        return;
    }

    private async Task PublishAsync(
        ConsumeResult<string, byte[]> record,
        string destination,
        CancellationToken cancellationToken,
        byte[]? body = null,
        DateTimeOffset? notBefore = null)
    {
        var headers = notBefore is null
            ? record.Message.Headers
            : KafkaMessageHeaders.CopyWithNotBefore(record.Message.Headers, notBefore.Value);
        await _retryProducer.ProduceAsync(destination, new Message<string, byte[]>
        {
            Key = record.Message.Key,
            Value = body ?? record.Message.Value,
            Headers = headers
        }, cancellationToken);
    }

    private static async Task WaitForRetryDueAsync(Headers? headers, CancellationToken cancellationToken)
    {
        var notBefore = KafkaMessageHeaders.GetNotBefore(headers);
        if (notBefore is { } due && due > DateTimeOffset.UtcNow)
        {
            await Task.Delay(due - DateTimeOffset.UtcNow, cancellationToken);
        }
    }

}
