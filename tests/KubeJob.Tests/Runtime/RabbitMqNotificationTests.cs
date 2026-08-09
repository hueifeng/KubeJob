using FluentAssertions;
using KubeJob.Core.Transport;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Tests.Runtime;

public sealed class RabbitMqNotificationTests
{
    [Fact]
    public void Control_plane_registration_replaces_default_notifier()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.UseRabbitMqKubeJobNotifications(options =>
            options.ConnectionString = "amqp://guest:guest@localhost:5672/");

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IWorkAvailableNotifier>()
            .Should().BeOfType<RabbitMqWorkAvailableNotifier>();
    }

    [Fact]
    public void Broker_native_registration_adds_transport_publisher_without_managed_runtime_client()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMqKubeJobBrokerNativeTransport(options =>
            options.ConnectionString = "amqp://guest:guest@localhost:5672/");

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IMessageTransportPublisher>()
            .Should().ContainSingle(publisher => publisher is RabbitMqBrokerNativePublisher);
        provider.GetService<IWorkerRuntimeClient>().Should().BeNull();
    }

    [Fact]
    public async Task Broker_native_submission_rejects_managed_idempotency_and_missing_transport_capabilities()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.ConfigureKubeJobQueueRuntimes(options =>
        {
            options.Queues["orders"] = new QueueRuntimeRoute
            {
                Mode = QueueRuntimeMode.BrokerNative,
                TransportId = "test"
            };
        });
        var publisher = new TestTransportPublisher(MessageTransportCapabilities.None);
        services.AddSingleton<IMessageTransportPublisher>(publisher);

        using var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<KubeJob.Core.Client.IJobClient>();
        var job = new KubeJob.Core.Jobs.JobKey<string>("orders");

        var idempotency = async () => await client.EnqueueAsync(
            job,
            "payload",
            new KubeJob.Core.Client.JobEnqueueOptions
            {
                IdempotencyKey = "order:42"
            });
        await idempotency.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*IdempotencyKey*");

        var durablePublish = async () => await client.EnqueueAsync(job, "payload");
        await durablePublish.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*DurablePublish*");
        publisher.PublishCount.Should().Be(0);
    }

    [Fact]
    public void Business_ingress_registration_keeps_the_control_plane_ingress_seam()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.AddRabbitMqKubeJobIngress(options =>
            options.ConnectionString = "amqp://guest:guest@localhost:5672/");

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IJobMessageIngress>()
            .Should().BeOfType<JobMessageIngress>();
    }

    private sealed class TestTransportPublisher : IMessageTransportPublisher
    {
        public TestTransportPublisher(MessageTransportCapabilities capabilities)
        {
            Capabilities = capabilities;
        }

        public string TransportId => "test";

        public MessageTransportCapabilities Capabilities { get; }

        public int PublishCount { get; private set; }

        public ValueTask PublishAsync(
            TransportPublishRequest request,
            CancellationToken cancellationToken = default)
        {
            PublishCount++;
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public void Remote_worker_notification_registration_keeps_http_claims_and_adds_shared_trigger()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobWorker(options =>
        {
            options.ServerEndpoint = "https://jobs.internal/";
            options.WorkerId = "worker-1";
            options.MaxConcurrentJobs = 1;
            options.Queues = new List<string> { "test.queue" };
        });
        services.AddRabbitMqKubeJobWorkerNotifications(options =>
            options.ConnectionString = "amqp://guest:guest@localhost:5672/");

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IWorkerRuntimeClient>()
            .Should().BeOfType<HttpWorkerRuntimeClient>();
        provider.GetRequiredService<IWorkerClaimTrigger>()
            .Should().BeSameAs(provider.GetRequiredService<IWorkerClaimTriggerSource>());
    }

    [Theory]
    [InlineData("")]
    [InlineData("http://rabbitmq")]
    [InlineData("rabbitmq")]
    public void Invalid_notification_connection_string_is_rejected(string value)
    {
        var options = new RabbitMqNotificationOptions { ConnectionString = value };
        options.Invoking(x => x.Validate()).Should().Throw<InvalidOperationException>();
    }

    [Theory]
    [InlineData("")]
    [InlineData("http://rabbitmq")]
    [InlineData("rabbitmq")]
    public void Invalid_broker_native_connection_string_is_rejected(string value)
    {
        var options = new RabbitMqBrokerNativeOptions { ConnectionString = value };
        options.Invoking(x => x.Validate()).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task Claim_trigger_is_bounded_and_coalesces_duplicate_pulses()
    {
        using var trigger = new WorkerClaimTrigger();
        trigger.Pulse();
        trigger.Pulse();
        trigger.Pulse();

        await trigger.WaitAsync(TimeSpan.FromMilliseconds(10), CancellationToken.None);
        var wait = trigger.WaitAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
        await Task.Delay(20);

        wait.IsCompleted.Should().BeFalse();
        trigger.Pulse();
        await wait;
    }

    [Fact]
    public void Notification_consumer_topology_options_are_validated()
    {
        var options = new RabbitMqNotificationOptions
        {
            ConsumerGroup = " ",
            PublisherConfirmTimeout = TimeSpan.Zero
        };

        options.Invoking(x => x.Validate()).Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Notification_queue_prefix_is_bounded_by_utf8_size()
    {
        var options = new RabbitMqNotificationOptions
        {
            ConsumerQueuePrefix = new string('队', 61)
        };

        options.Invoking(x => x.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*180 UTF-8 bytes*");
    }

    [Fact]
    public void Broker_native_uses_one_physical_queue_per_logical_job_queue()
    {
        var options = new RabbitMqBrokerNativeOptions();

        options.GetQueueName("mail.send").Should().Be("kubejob.mail.send");
        options.GetQueueName("report.generate").Should().Be("kubejob.report.generate");
        options.GetQueueName("mail.send").Should().NotBe(options.GetQueueName("report.generate"));
    }

    [Fact]
    public void Event_subscriptions_get_independent_queues_under_one_topic_exchange()
    {
        var options = new RabbitMqBrokerNativeOptions();

        options.GetEventExchangeName("order.events").Should().Be("kubejob.order.events");
        options.GetEventSubscriptionQueueName("order.events", "order-business")
            .Should().Be("kubejob.order.events.order-business");
        options.GetEventSubscriptionQueueName("order.events", "order-log")
            .Should().Be("kubejob.order.events.order-log");
    }

    [Fact]
    public void Broker_native_dispatch_concurrency_is_bounded()
    {
        var options = new RabbitMqBrokerNativeOptions
        {
            ConsumerDispatchConcurrency = 257
        };

        options.Invoking(x => x.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*ConsumerDispatchConcurrency*");
    }

    [Fact]
    public void Broker_native_validates_actual_generated_topology_names()
    {
        var options = new RabbitMqBrokerNativeOptions
        {
            QueuePrefix = new string('q', 244)
        };

        options.Invoking(x => x.GetQueueName("orders.push"))
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*255 UTF-8 bytes*");
    }

    [Fact]
    public void Business_ingress_options_validate_broker_topology()
    {
        var options = new RabbitMqJobIngressOptions
        {
            QueueName = " ",
            Source = "orders"
        };

        options.Invoking(x => x.Validate()).Should().Throw<ArgumentException>();
    }
}
