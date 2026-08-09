using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Core.Transport;
using KubeJob.Transport.Kafka;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Tests.Runtime;

public sealed class KafkaBrokerNativeOptionsTests
{
    [Fact]
    public void Fixed_event_capabilities_map_to_independent_consumer_groups_and_topics()
    {
        var options = new KafkaBrokerNativeOptions { Environment = "production" };

        options.GetEventConsumerGroup("log").Should().Be("kubejob.production.log");
        options.GetEventConsumerGroup("data").Should().Be("kubejob.production.data");
        options.GetEventConsumerGroup("notify").Should().Be("kubejob.production.notify");
        options.GetEventRetryTopic("data").Should().Be("order.events.data.retry");
        options.GetEventDeadLetterTopic("notify").Should().Be("order.events.notify.dlq");
    }

    [Fact]
    public void Unsupported_event_capability_is_rejected()
    {
        var options = new KafkaBrokerNativeOptions();

        options.Invoking(x => x.GetEventConsumerGroup("analytics"))
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*log, data, or notify*");
    }

    [Fact]
    public void Job_queue_has_dedicated_main_retry_and_dead_letter_topics()
    {
        var options = new KafkaBrokerNativeOptions();

        options.GetJobTopic("orders.created").Should().Be("kubejob.jobs.orders.created");
        options.GetJobRetryTopic("orders.created").Should().Be("kubejob.jobs.orders.created.retry");
        options.GetJobDeadLetterTopic("orders.created").Should().Be("kubejob.jobs.orders.created.dlq");
    }

    [Theory]
    [InlineData(1, 5)]
    [InlineData(9, 30)]
    [InlineData(31, 300)]
    [InlineData(301, 1800)]
    [InlineData(7200, 1800)]
    public void Retry_delay_is_rounded_to_a_bounded_kafka_tier(int requestedSeconds, int expectedSeconds)
    {
        var options = new KafkaBrokerNativeOptions();
        var policy = new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(requestedSeconds), TimeSpan.FromSeconds(requestedSeconds));

        options.GetRetryDelay(policy, failedAttempt: 1).Should().Be(TimeSpan.FromSeconds(expectedSeconds));
    }

    [Theory]
    [InlineData(BackoffStrategy.Fixed, 5)]
    [InlineData(BackoffStrategy.Linear, 30)]
    [InlineData(BackoffStrategy.Exponential, 30)]
    public void Retry_delay_advances_from_the_attempt_that_failed(BackoffStrategy strategy, int expectedSeconds)
    {
        var options = new KafkaBrokerNativeOptions();
        var policy = new RetryPolicy(strategy, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(30));

        options.GetRetryDelay(policy, failedAttempt: 2).Should().Be(TimeSpan.FromSeconds(expectedSeconds));
    }

    [Theory]
    [InlineData("not-a-protocol", null)]
    [InlineData("SaslSsl", "not-a-mechanism")]
    public void Invalid_security_configuration_is_rejected(string protocol, string? mechanism)
    {
        var options = new KafkaBrokerNativeOptions
        {
            SecurityProtocol = protocol,
            SaslMechanism = mechanism
        };

        options.Invoking(x => x.Validate()).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Transport_registration_keeps_the_core_publisher_seam()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKafkaKubeJobBrokerNativeTransport(options => options.BootstrapServers = "localhost:9092");

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IMessageTransportPublisher>()
            .Should().ContainSingle(publisher => publisher is KafkaBrokerNativePublisher);
        provider.GetRequiredService<IMessageTransportPublisher>().TransportId
            .Should().Be(KafkaBrokerNativePublisher.Id);
    }
}
