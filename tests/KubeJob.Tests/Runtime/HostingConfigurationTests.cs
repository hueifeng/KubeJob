using FluentAssertions;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Storage.PostgreSQL.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KubeJob.Tests.Runtime;

public sealed class HostingConfigurationTests
{
    [Fact]
    public void Server_registers_the_complete_V2_control_plane()
    {
        var services = new ServiceCollection();

        services.AddKubeJobServer();

        HasHostedService<ScheduleReconcilerService>(services).Should().BeTrue();
        HasHostedService<LeaseReaperService>(services).Should().BeTrue();
        HasHostedService<OutboxPublisherService>(services).Should().BeTrue();
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

    private static bool HasHostedService<THostedService>(IServiceCollection services)
        where THostedService : class, IHostedService
        => services.Any(descriptor =>
            descriptor.ServiceType == typeof(IHostedService)
            && descriptor.ImplementationType == typeof(THostedService));
}
