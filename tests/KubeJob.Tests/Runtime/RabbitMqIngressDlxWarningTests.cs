using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Transport.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Locks in the hard-fail behavior: <see cref="RabbitMqJobIngressService"/>
/// refuses to start without a <c>DeadLetterExchangeName</c> because permanent
/// rejects (malformed JSON, validation errors, idempotency conflicts) would
/// otherwise be silently dropped by the broker. Operators who genuinely want
/// to opt out must set <c>AllowNoDeadLetterExchange=true</c>.
/// </summary>
public sealed class RabbitMqIngressDlxWarningTests
{
    [Fact]
    public void Constructor_throws_when_dead_letter_exchange_is_not_configured()
    {
        var options = Options.Create(new RabbitMqJobIngressOptions
        {
            ConnectionString = "amqp://guest:guest@localhost:5672/",
            // DeadLetterExchangeName intentionally null
            DeadLetterExchangeName = null
        });

        var ingress = BuildIngress();

        var action = () => new RabbitMqJobIngressService(
            options,
            ingress,
            NullLogger<RabbitMqJobIngressService>.Instance);

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*DeadLetterExchangeName*required*");
    }

    [Fact]
    public void Constructor_throws_when_only_routing_key_is_configured()
    {
        var options = Options.Create(new RabbitMqJobIngressOptions
        {
            ConnectionString = "amqp://guest:guest@localhost:5672/",
            DeadLetterRoutingKey = "kubejob.ingress.dead"
        });

        var ingress = BuildIngress();

        var action = () => new RabbitMqJobIngressService(
            options,
            ingress,
            NullLogger<RabbitMqJobIngressService>.Instance);

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*DeadLetterRoutingKey requires DeadLetterExchangeName*");
    }

    [Fact]
    public void Constructor_succeeds_when_dead_letter_exchange_is_configured()
    {
        var options = Options.Create(new RabbitMqJobIngressOptions
        {
            ConnectionString = "amqp://guest:guest@localhost:5672/",
            DeadLetterExchangeName = "kubejob.ingress.dlx",
            DeadLetterRoutingKey = "kubejob.ingress.dead"
        });

        var ingress = BuildIngress();

        var action = () => new RabbitMqJobIngressService(
            options,
            ingress,
            NullLogger<RabbitMqJobIngressService>.Instance);

        action.Should().NotThrow();
    }

    [Fact]
    public void Constructor_succeeds_with_explicit_opt_out()
    {
        var options = Options.Create(new RabbitMqJobIngressOptions
        {
            ConnectionString = "amqp://guest:guest@localhost:5672/",
            AllowNoDeadLetterExchange = true
        });

        var ingress = BuildIngress();

        var action = () => new RabbitMqJobIngressService(
            options,
            ingress,
            NullLogger<RabbitMqJobIngressService>.Instance);

        action.Should().NotThrow();
    }

    private static IJobMessageIngress BuildIngress()
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddKubeJobServer();
        return services.BuildServiceProvider().GetRequiredService<IJobMessageIngress>();
    }
}
