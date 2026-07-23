using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
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
    public void Remote_worker_registration_decorates_http_runtime_client()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobWorkerRuntime(options =>
        {
            options.ServerEndpoint = "https://jobs.internal/";
            options.WorkerId = "worker-1";
            options.MaxConcurrentJobs = 1;
        });
        services.AddRabbitMqKubeJobWorkerNotifications(options =>
            options.ConnectionString = "amqp://guest:guest@localhost:5672/");

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IWorkerRuntimeClient>()
            .Should().BeOfType<NotificationAwareWorkerRuntimeClient>();
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
    public async Task Wake_signal_is_bounded_and_coalesces_duplicates()
    {
        using var signal = new WorkerWakeSignal();
        signal.Pulse();
        signal.Pulse();
        signal.Pulse();

        (await signal.WaitAsync(TimeSpan.Zero, CancellationToken.None)).Should().BeTrue();
        (await signal.WaitAsync(TimeSpan.Zero, CancellationToken.None)).Should().BeFalse();
    }
}
