using System.Text;
using System.Text.Json;
using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Transport.RabbitMQ;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KubeJob.RabbitMqIntegrationTests;

/// <summary>
/// Integration tests for the fixed-N execution lane topology against a live
/// RabbitMQ broker. Gated on the KUBEJOB_RABBITMQ_TEST_CONNECTION environment
/// variable (mirrors the existing integration tests), so a plain
/// <c>dotnet test</c> without a broker available throws rather than runs.
/// </summary>
[Collection(RabbitMqIntegrationCollection.Name)]
public sealed class RabbitMqExecutionLaneIntegrationTests
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    [Fact]
    public async Task Same_partition_key_co_locates_on_one_lane_queue()
    {
        var connectionString = RequireConnectionString();
        var group = $"lane-colocate-{Guid.NewGuid():N}";
        var options = NewOptions(connectionString, group, laneCount: 4);

        using var dispatcher = new RabbitMqExecutionDispatcher(Options.Create(options));
        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        try
        {
            DeclareLaneTopology(channel, options, new[] { "default" });

            const string partitionKey = "tenant-A";
            var expectedLane = ExecutionLaneRouter.GetLane(partitionKey, options.ExecutionLaneCount);

            for (var i = 0; i < 5; i++)
            {
                await dispatcher.PublishAsync(
                    Envelope($"event-{i}", "default", partitionKey, options.ConsumerGroup),
                    CancellationToken.None);
            }

            await EventuallyAsync(() =>
                Task.FromResult(channel.MessageCount(options.GetConsumerQueueName("default", expectedLane)) == 5));

            // The other lanes must stay empty: every same-key run landed on
            // the expected lane only.
            for (var lane = 0; lane < options.ExecutionLaneCount; lane++)
            {
                if (lane == expectedLane)
                {
                    continue;
                }

                channel.MessageCount(options.GetConsumerQueueName("default", lane))
                    .Should().Be(0);
            }
        }
        finally
        {
            DeleteLaneTopology(channel, options, new[] { "default" });
        }
    }

    [Fact]
    public async Task Distinct_partition_keys_spread_across_lanes()
    {
        var connectionString = RequireConnectionString();
        var group = $"lane-spread-{Guid.NewGuid():N}";
        var options = NewOptions(connectionString, group, laneCount: 4);

        using var dispatcher = new RabbitMqExecutionDispatcher(Options.Create(options));
        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        try
        {
            DeclareLaneTopology(channel, options, new[] { "default" });

            var keys = Enumerable.Range(0, 16).Select(i => $"tenant-{i}").ToArray();
            foreach (var key in keys)
            {
                await dispatcher.PublishAsync(
                    Envelope($"event-{key}", "default", key, options.ConsumerGroup),
                    CancellationToken.None);
            }

            // Build the expected per-lane distribution directly from the router
            // (the broker is not the authority; the lane is deterministic) and
            // assert the broker depth matches it lane-for-lane.
            var expected = new int[options.ExecutionLaneCount];
            foreach (var key in keys)
            {
                expected[ExecutionLaneRouter.GetLane(key, options.ExecutionLaneCount)]++;
            }

            await EventuallyAsync(() =>
                Task.FromResult(
                    Enumerable.Range(0, options.ExecutionLaneCount)
                        .All(lane => channel.MessageCount(options.GetConsumerQueueName("default", lane)) == expected[lane])));

            // Spread sanity: with 16 distinct keys across 4 lanes, more than
            // one lane must be populated (i.e. keys are not all collapsing to
            // a single lane).
            expected.Count(count => count > 0).Should().BeGreaterThan(1);
        }
        finally
        {
            DeleteLaneTopology(channel, options, new[] { "default" });
        }
    }

    [Fact]
    public async Task Broker_retry_dead_letter_re_lands_on_the_same_lane_queue()
    {
        var connectionString = RequireConnectionString();
        var group = $"lane-retry-{Guid.NewGuid():N}";
        var options = NewOptions(connectionString, group, laneCount: 4);
        options.RetryDelay = TimeSpan.FromSeconds(3);

        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        try
        {
            DeclareLaneTopology(channel, options, new[] { "default" });

            const int lane = 2;
            var retryQueue = options.GetSharedRetryQueueName();
            var dispatchQueue = options.GetConsumerQueueName("default", lane);

            // Simulate the consumer's retry republish: publish the envelope to
            // the retry exchange with the lane-suffixed routing key. The retry
            // queue does not set x-dead-letter-routing-key, so RabbitMQ must
            // preserve this key on the TTL dead-letter and route the message
            // back onto the same lane's dispatch queue.
            var envelope = Envelope($"event-retry-{Guid.NewGuid():N}", "default", "tenant-retry", options.ConsumerGroup);
            var properties = channel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.DeliveryMode = 2;
            properties.MessageId = envelope.EventId;
            channel.BasicPublish(
                options.GetRetryExchangeName(),
                options.GetLaneRoutingKey("default", lane),
                mandatory: true,
                basicProperties: properties,
                body: Encoding.UTF8.GetBytes(JsonSerializer.Serialize(envelope, SerializerOptions)));

            // The publish lands asynchronously (client write buffer + quorum
            // visibility), so wait for it before asserting.
            await EventuallyAsync(() =>
                Task.FromResult(channel.MessageCount(retryQueue) == 1));

            await EventuallyAsync(() =>
                Task.FromResult(channel.MessageCount(retryQueue) == 0));

            // The retried message re-landed on the SAME lane dispatch queue,
            // and the other lanes did not receive it.
            await EventuallyAsync(() =>
                Task.FromResult(channel.MessageCount(dispatchQueue) == 1));
            channel.MessageCount(dispatchQueue).Should().Be(1);
            for (var other = 0; other < options.ExecutionLaneCount; other++)
            {
                if (other == lane)
                {
                    continue;
                }

                channel.MessageCount(options.GetConsumerQueueName("default", other))
                    .Should().Be(0);
            }
        }
        finally
        {
            DeleteLaneTopology(channel, options, new[] { "default" });
        }
    }

    [Fact]
    public async Task Single_lane_routes_to_today_s_shared_queue_name()
    {
        var connectionString = RequireConnectionString();
        var group = $"lane-n1-{Guid.NewGuid():N}";
        var options = NewOptions(connectionString, group, laneCount: 1);

        using var dispatcher = new RabbitMqExecutionDispatcher(Options.Create(options));
        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        try
        {
            DeclareLaneTopology(channel, options, new[] { "default" });

            // N=1 must publish to the pre-lane queue name: no lane suffix on
            // the queue name or the routing key.
            var sharedQueue = options.GetConsumerQueueName("default");
            sharedQueue.Should().NotEndWith(".lane-0");
            options.GetLaneRoutingKey("default", 0).Should().Be("default");

            await dispatcher.PublishAsync(
                Envelope($"event-n1-{Guid.NewGuid():N}", "default", "any-tenant", options.ConsumerGroup),
                CancellationToken.None);

            await EventuallyAsync(() =>
                Task.FromResult(channel.MessageCount(sharedQueue) == 1));
        }
        finally
        {
            DeleteLaneTopology(channel, options, new[] { "default" });
        }
    }

    [Fact]
    public async Task Non_shared_topology_uses_literal_business_queue_names()
    {
        var connectionString = RequireConnectionString();
        var group = $"business-queues-{Guid.NewGuid():N}";
        var options = NewOptions(connectionString, group, laneCount: 1);
        var logicalQueues = new[] { "mail.send", "report.generate" };

        using var dispatcher = new RabbitMqExecutionDispatcher(Options.Create(options));
        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        try
        {
            DeclareLaneTopology(channel, options, logicalQueues);
            var mailQueue = options.GetConsumerQueueName("mail.send");
            var reportQueue = options.GetConsumerQueueName("report.generate");
            mailQueue.Should().Be($"kubejob.test.execution.{group}.mail.send.queue");
            reportQueue.Should().Be($"kubejob.test.execution.{group}.report.generate.queue");
            options.GetSharedRetryQueueName()
                .Should().Be($"kubejob.test.execution.{group}.retry.queue");
            options.GetSharedRetryQueueName()
                .Should().Be(options.GetSharedRetryQueueName());

            await dispatcher.PublishAsync(
                Envelope($"event-business-{Guid.NewGuid():N}", "mail.send", "tenant-a", options.ConsumerGroup),
                CancellationToken.None);

            await EventuallyAsync(() => Task.FromResult(channel.MessageCount(mailQueue) == 1));
            channel.MessageCount(reportQueue).Should().Be(0);
        }
        finally
        {
            DeleteLaneTopology(channel, options, logicalQueues);
        }
    }

    private static RabbitMqExecutionOptions NewOptions(
        string connectionString,
        string group,
        int laneCount) =>
        new()
        {
            ConnectionString = connectionString,
            ConsumerGroup = group,
            ConsumerQueuePrefix = "kubejob.test.execution",
            ExecutionLaneCount = laneCount,
            PublisherConfirmTimeout = TimeSpan.FromSeconds(5),
            RetryDelay = TimeSpan.FromMilliseconds(300),
        };

    private static ExecutionEnvelope Envelope(string eventId, string queue, string partitionKey, string consumerGroup) =>
        new()
        {
            SchemaVersion = ExecutionEnvelope.CurrentSchemaVersion,
            EventId = eventId,
            Queue = queue,
            ExecutionLane = "default",
            ConsumerGroup = consumerGroup,
            RunId = $"run-{eventId}",
            PartitionKey = partitionKey
        };

    private static string RequireConnectionString() =>
        Environment.GetEnvironmentVariable("KUBEJOB_RABBITMQ_TEST_CONNECTION")
            ?? throw new InvalidOperationException(
                "Set KUBEJOB_RABBITMQ_TEST_CONNECTION before running this integration project.");

    private static IConnection CreateConnection(string connectionString) =>
        new ConnectionFactory
        {
            Uri = new Uri(connectionString, UriKind.Absolute)
        }.CreateConnection("KubeJob.Tests.RabbitMqExecutionLane");

    /// <summary>
    /// Declares the lane topology inline (group exchange, retry exchange, group
    /// DLX/DLQ, and per-lane dispatch + retry queues bound with lane-suffixed
    /// routing keys). Mirrors <see cref="RabbitMqTopologyProvisioner.DeclareTopology"/>
    /// without requiring a hosted worker.
    /// </summary>
    private static void DeclareLaneTopology(
        IModel channel,
        RabbitMqExecutionOptions options,
        IReadOnlyList<string> logicalQueues)
    {
        channel.ExchangeDeclare(options.GetGroupExchangeName(), ExchangeType.Direct, durable: true, autoDelete: false);
        channel.ExchangeDeclare(options.GetRetryExchangeName(), ExchangeType.Direct, durable: true, autoDelete: false);
        channel.ExchangeDeclare(options.GetGroupDlxName(), ExchangeType.Fanout, durable: true, autoDelete: false);
        channel.QueueDeclare(
            options.GetGroupDlqName(),
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object> { ["x-queue-type"] = "quorum" });
        channel.QueueBind(options.GetGroupDlqName(), options.GetGroupDlxName(), routingKey: string.Empty);

        var retryArguments = new Dictionary<string, object>
        {
            ["x-queue-type"] = "quorum",
            ["x-message-ttl"] = checked((int)options.RetryDelay.TotalMilliseconds),
            // No x-dead-letter-routing-key: the original (lane-suffixed) routing
            // key is preserved on dead-letter so the retried message re-lands on
            // the same lane dispatch queue.
            ["x-dead-letter-exchange"] = options.GetGroupExchangeName(),
        };
        var dispatchArguments = new Dictionary<string, object>
        {
            ["x-queue-type"] = "quorum",
            ["x-dead-letter-exchange"] = options.GetGroupDlxName(),
        };
        var retryQueue = options.GetSharedRetryQueueName();
        channel.QueueDeclare(retryQueue, durable: true, exclusive: false, autoDelete: false, retryArguments);

        for (var lane = 0; lane < options.ExecutionLaneCount; lane++)
        {
            foreach (var logicalQueue in logicalQueues)
            {
                var dispatchQueue = options.GetConsumerQueueName(logicalQueue, lane);
                channel.QueueDeclare(dispatchQueue, durable: true, exclusive: false, autoDelete: false, dispatchArguments);
                channel.QueueBind(dispatchQueue, options.GetGroupExchangeName(), options.GetLaneRoutingKey(logicalQueue, lane));

                channel.QueueBind(retryQueue, options.GetRetryExchangeName(), options.GetLaneRoutingKey(logicalQueue, lane));
            }
        }
    }

    private static void DeleteLaneTopology(
        IModel channel,
        RabbitMqExecutionOptions options,
        IReadOnlyList<string> logicalQueues)
    {
        try
        {
            var deletedRetryQueues = new HashSet<string>(StringComparer.Ordinal);
            for (var lane = 0; lane < options.ExecutionLaneCount; lane++)
            {
                foreach (var logicalQueue in logicalQueues)
                {
                    channel.QueueDelete(options.GetConsumerQueueName(logicalQueue, lane), ifUnused: false, ifEmpty: false);
                    if (deletedRetryQueues.Add(options.GetSharedRetryQueueName()))
                    {
                        channel.QueueDelete(options.GetSharedRetryQueueName(), ifUnused: false, ifEmpty: false);
                    }
                }
            }

            channel.QueueDelete(options.GetGroupDlqName(), ifUnused: false, ifEmpty: false);
            channel.ExchangeDelete(options.GetGroupExchangeName());
            channel.ExchangeDelete(options.GetRetryExchangeName());
            channel.ExchangeDelete(options.GetGroupDlxName());
        }
        catch (Exception)
        {
            // Best-effort cleanup; stray broker topology does not fail the run.
        }
    }

    private static async Task EventuallyAsync(Func<Task<bool>> condition, int attempts = 200)
    {
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(100);
        }

        (await condition()).Should().BeTrue();
    }
}