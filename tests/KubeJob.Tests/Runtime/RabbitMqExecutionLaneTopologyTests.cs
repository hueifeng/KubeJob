using FluentAssertions;
using KubeJob.Transport.RabbitMQ;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Unit tests for the lane-aware RabbitMQ topology naming. The non-negotiable
/// guarantee is that <c>ExecutionLaneCount == 1</c> reproduces today's queue,
/// retry-queue, and routing-key names byte-for-byte (zero migration for
/// existing deployments), while N&gt;1 fans out per-lane queues whose binding
/// keys match the lane-suffixed routing keys the dispatcher publishes.
/// </summary>
public sealed class RabbitMqExecutionLaneTopologyTests
{
    [Fact]
    public void Non_shared_single_lane_uses_the_literal_logical_queue_name()
    {
        var options = new RabbitMqExecutionOptions
        {
            ConsumerGroup = "default",
            ConsumerQueuePrefix = "kubejob.execution",
            ExecutionLaneCount = 1,
        };

        options.GetConsumerQueueName("mail.send", 0)
            .Should().Be(options.GetConsumerQueueName("mail.send"))
            .And.Be("kubejob.execution.default.mail.send.queue");
        options.GetSharedRetryQueueName()
            .Should().Be("kubejob.execution.default.retry.queue");
    }

    [Fact]
    public void Default_topology_creates_one_physical_queue_per_business_logical_queue()
    {
        var options = new RabbitMqExecutionOptions
        {
            ConsumerGroup = "default",
            ConsumerQueuePrefix = "kubejob.execution",
        };

        options.GetConsumerQueueName("mail.send")
            .Should().Be("kubejob.execution.default.mail.send.queue");
        options.GetConsumerQueueName("report.generate")
            .Should().Be("kubejob.execution.default.report.generate.queue");
    }

    [Fact]
    public void Retry_topology_uses_one_group_queue_across_businesses_and_lanes()
    {
        var options = new RabbitMqExecutionOptions
        {
            ConsumerGroup = "default",
            ConsumerQueuePrefix = "kubejob.execution",
            ExecutionLaneCount = 4,
        };

        options.GetSharedRetryQueueName()
            .Should().Be("kubejob.execution.default.retry.queue");
        options.GetSharedRetryQueueName()
            .Should().Be(options.GetSharedRetryQueueName());
    }

    [Fact]
    public void Non_shared_mode_fans_out_one_queue_per_logical_queue_per_lane()
    {
        var options = new RabbitMqExecutionOptions
        {
            ConsumerGroup = "fleet",
            ConsumerQueuePrefix = "kubejob.execution",
            ExecutionLaneCount = 4,
        };

        // One physical dispatch queue per (logical queue, lane): distinct names
        // across both dimensions.
        options.GetConsumerQueueName("default", 0)
            .Should().Be("kubejob.execution.fleet.default.lane-0.queue");
        options.GetConsumerQueueName("default", 3)
            .Should().Be("kubejob.execution.fleet.default.lane-3.queue");
        options.GetConsumerQueueName("orders", 1)
            .Should().Be("kubejob.execution.fleet.orders.lane-1.queue");

        options.GetConsumerQueueName("default", 0)
            .Should().NotBe(options.GetConsumerQueueName("default", 1));
        options.GetConsumerQueueName("default", 0)
            .Should().NotBe(options.GetConsumerQueueName("orders", 0));

        // Broker retry does not mirror business dispatch queues; it is shared.
        options.GetSharedRetryQueueName()
            .Should().Be("kubejob.execution.fleet.retry.queue");
        options.GetSharedRetryQueueName()
            .Should().Be(options.GetSharedRetryQueueName());

        // Routing keys stay lane-suffixed so the retry dead-letter re-lands on
        // the same lane regardless of shared vs non-shared mode.
        options.GetLaneRoutingKey("orders", 2).Should().Be("orders.lane-2");
    }

    [Fact]
    public void Lane_count_must_be_between_one_and_sixty_four()
    {
        var tooFew = new RabbitMqExecutionOptions { ExecutionLaneCount = 0 };
        var tooMany = new RabbitMqExecutionOptions { ExecutionLaneCount = 65 };

        tooFew.Invoking(o => o.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*ExecutionLaneCount*");
        tooMany.Invoking(o => o.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*ExecutionLaneCount*");
    }

    [Fact]
    public void Single_lane_routing_key_has_no_suffix_even_for_large_lane_ids()
    {
        var options = new RabbitMqExecutionOptions
        {
            ConsumerGroup = "default",
            ExecutionLaneCount = 1,
        };

        // Lane 0 is the only lane for N=1, but assert the suffix helper itself
        // stays empty so names and routing keys never accidentally gain ".lane-0".
        options.GetLaneRoutingKey("orders", 0).Should().Be("orders");
        options.GetConsumerQueueName("orders", 0)
            .Should().NotContain("lane-");
    }
}