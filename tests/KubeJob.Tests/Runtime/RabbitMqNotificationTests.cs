using FluentAssertions;
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
    public void Control_plane_can_register_a_separate_execution_dispatcher()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.UseRabbitMqKubeJobExecutionDispatcher(options =>
            options.ConnectionString = "amqp://guest:guest@localhost:5672/");

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IExecutionDispatcher>()
            .Should().BeOfType<RabbitMqExecutionDispatcher>();
    }

    [Fact]
    public void Worker_can_register_execution_consumer_without_changing_runtime_client()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobWorker(options =>
        {
            options.ServerEndpoint = "https://jobs.internal/";
            options.WorkerId = "worker-1";
            options.MaxConcurrentJobs = 1;
        });
        services.AddRabbitMqKubeJobExecutionConsumer(options =>
            options.ConnectionString = "amqp://guest:guest@localhost:5672/");

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IWorkerRuntimeClient>()
            .Should().BeOfType<HttpWorkerRuntimeClient>();
        provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>()
            .Should().Contain(service => service is RabbitMqExecutionConsumerService);
    }

    [Theory]
    [InlineData("")]
    [InlineData("http://rabbitmq")]
    [InlineData("rabbitmq")]
    public void Invalid_execution_connection_string_is_rejected(string value)
    {
        var options = new RabbitMqExecutionOptions
        {
            ConnectionString = value
        };

        var action = options.Validate;

        action.Should().Throw<InvalidOperationException>();
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

    [Fact]
    public void Remote_worker_registration_keeps_http_claims_and_adds_shared_trigger()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobWorker(options =>
        {
            options.ServerEndpoint = "https://jobs.internal/";
            options.WorkerId = "worker-1";
            options.MaxConcurrentJobs = 1;
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
    public void Invalid_connection_string_is_rejected(string value)
    {
        var options = new RabbitMqNotificationOptions
        {
            ConnectionString = value
        };

        var action = options.Validate;

        action.Should().Throw<InvalidOperationException>();
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
    public void Invalid_consumer_topology_options_are_rejected()
    {
        var options = new RabbitMqNotificationOptions
        {
            ConsumerGroup = " ",
            PublisherConfirmTimeout = TimeSpan.Zero
        };

        var action = options.Validate;

        action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Nonpositive_publisher_confirm_timeout_is_rejected()
    {
        var options = new RabbitMqNotificationOptions
        {
            PublisherConfirmTimeout = TimeSpan.Zero
        };

        var action = options.Validate;

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*PublisherConfirmTimeout*");
    }

    [Fact]
    public void Consumer_queue_prefix_is_bounded_by_utf8_size()
    {
        var options = new RabbitMqNotificationOptions
        {
            ConsumerQueuePrefix = new string('队', 61)
        };

        var action = options.Validate;

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*180 UTF-8 bytes*");
    }

    [Fact]
    public void Business_ingress_options_validate_broker_topology()
    {
        var options = new RabbitMqJobIngressOptions
        {
            QueueName = " ",
            Source = "orders"
        };

        var action = options.Validate;

        action.Should().Throw<ArgumentException>();
    }
}
