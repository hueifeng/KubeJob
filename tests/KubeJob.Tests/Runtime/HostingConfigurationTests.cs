using FluentAssertions;
using KubeJob.Server.Extensions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Storage.PostgreSQL.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace KubeJob.Tests.Runtime;

public sealed class HostingConfigurationTests
{
    [Fact]
    public void Server_registers_the_complete_control_plane()
    {
        var services = new ServiceCollection();

        services.AddKubeJobServer();

        HasHostedService<ScheduleReconcilerService>(services).Should().BeTrue();
        HasHostedService<LeaseReaperService>(services).Should().BeTrue();
        HasHostedService<TimeoutScannerService>(services).Should().BeTrue();
        HasHostedService<OutboxPublisherService>(services).Should().BeTrue();
        HasHostedService<RuntimeRetentionService>(services).Should().BeTrue();
    }

    [Fact]
    public async Task Server_registers_a_runtime_readiness_check()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        using var provider = services.BuildServiceProvider();

        var result = await provider
            .GetRequiredService<HealthCheckService>()
            .CheckHealthAsync();

        result.Status.Should().Be(HealthStatus.Healthy);
        result.Entries.Should().ContainKey("kubejob-runtime");
    }

    [Fact]
    public void PostgreSQL_registration_uses_one_durable_store_for_runtime_and_dashboard()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer(options =>
            options.UsePostgreSql(
                "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres"));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IJobScheduleStore>()
            .Should().BeOfType<PostgreSqlJobRuntimeStore>();
        provider.GetRequiredService<IJobRuntimeDashboardStore>()
            .Should().BeOfType<PostgreSqlJobRuntimeStore>();
    }

    [Fact]
    public void PostgreSQL_registration_fails_fast_when_background_pool_is_undersized()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer(options =>
            options.UsePostgreSql(
                "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres",
                postgres => postgres.BackgroundPoolSize = 1));

        using var provider = services.BuildServiceProvider();

        var act = () => provider.GetRequiredService<IJobScheduleStore>();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*BackgroundPoolSize*");
    }

    private static bool HasHostedService<THostedService>(IServiceCollection services)
        where THostedService : class, IHostedService
        => services.Any(descriptor =>
            descriptor.ServiceType == typeof(IHostedService)
            && descriptor.ImplementationType == typeof(THostedService));
}
