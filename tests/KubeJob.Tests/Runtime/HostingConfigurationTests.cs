using FluentAssertions;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Server.Services;
using KubeJob.Storage.PostgreSQL.Extensions;
using KubeJob.Storage.PostgreSQL.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KubeJob.Tests.Runtime;

public sealed class HostingConfigurationTests
{
    [Fact]
    public void V2_only_registers_only_V2_background_services()
    {
        var services = new ServiceCollection();

        services.AddKubeJobServer(options => options.UseV2Only());

        HasHostedService<ScheduleReconcilerService>(services).Should().BeTrue();
        HasHostedService<LeaseReaperService>(services).Should().BeTrue();
        HasHostedService<OutboxPublisherService>(services).Should().BeTrue();
        HasHostedService<CronSchedulerService>(services).Should().BeFalse();
        HasHostedService<JobDispatcherService>(services).Should().BeFalse();
        HasHostedService<NodeHealthService>(services).Should().BeFalse();
        HasHostedService<HistoryCleanupService>(services).Should().BeFalse();
    }

    [Fact]
    public void Legacy_only_registers_only_legacy_background_services()
    {
        var services = new ServiceCollection();

        services.AddKubeJobServer(options => options.UseLegacyOnly());

        HasHostedService<CronSchedulerService>(services).Should().BeTrue();
        HasHostedService<JobDispatcherService>(services).Should().BeTrue();
        HasHostedService<NodeHealthService>(services).Should().BeTrue();
        HasHostedService<HistoryCleanupService>(services).Should().BeTrue();
        HasHostedService<ScheduleReconcilerService>(services).Should().BeFalse();
        HasHostedService<LeaseReaperService>(services).Should().BeFalse();
        HasHostedService<OutboxPublisherService>(services).Should().BeFalse();
    }

    [Fact]
    public void PostgreSQL_registration_uses_the_durable_store_for_schedules()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer(options =>
            options.UseV2Only().UsePostgreSql(
                "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres"));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IJobScheduleStore>()
            .Should().BeOfType<PostgreSqlJobRuntimeStore>();
    }

    private static bool HasHostedService<THostedService>(IServiceCollection services)
        where THostedService : class, IHostedService
        => services.Any(descriptor =>
            descriptor.ServiceType == typeof(IHostedService)
            && descriptor.ImplementationType == typeof(THostedService));
}
