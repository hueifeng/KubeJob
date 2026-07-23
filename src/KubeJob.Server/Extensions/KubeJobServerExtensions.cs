using KubeJob.Core.Client;
using KubeJob.Core.Scheduling;
using KubeJob.Server.Options;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.ApplicationModels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Server.Extensions;

public static class KubeJobServerExtensions
{
    public static IServiceCollection AddKubeJobServer(
        this IServiceCollection services,
        Action<KubeJobServerOptions>? configure = null)
    {
        var options = new KubeJobServerOptions();
        configure?.Invoke(options);
        options.StorageConfigurator?.Invoke(services);

        services.TryAddSingleton<InMemoryJobRuntimeStore>();
        services.TryAddSingleton<IJobSubmissionStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IWorkerSessionStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobClaimStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobCompletionStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobQueryStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobScheduleStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IOutboxStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobRuntimeDashboardStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IWorkAvailableNotifier, PollingWorkAvailableNotifier>();
        services.TryAddSingleton<IJobClient, DefaultJobClient>();
        services.TryAddSingleton<IJobScheduleClient, DefaultJobScheduleClient>();
        services.AddOptions<JobRuntimeOptions>();

        services.AddControllers();
        services.AddHostedService<ScheduleReconcilerService>();
        services.AddHostedService<LeaseReaperService>();
        services.AddHostedService<OutboxPublisherService>();
        return services;
    }

    public static IServiceCollection AddKubeJobDashboard(
        this IServiceCollection services,
        string routePrefix = "kubejob")
    {
        services.AddControllersWithViews(options =>
        {
            options.Conventions.Add(new KubeJobDashboardRouteConvention(routePrefix));
        });
        return services;
    }

    public static void InitializeKubeJobDatabase(this IApplicationBuilder app)
    {
        using var scope = app.ApplicationServices.CreateScope();
        var initializer = scope.ServiceProvider.GetService<KubeJob.Server.Data.IStorageInitializer>();
        initializer?.Initialize();
    }
}

public sealed class KubeJobDashboardRouteConvention : IControllerModelConvention
{
    private readonly string _routePrefix;

    public KubeJobDashboardRouteConvention(string routePrefix)
    {
        _routePrefix = string.IsNullOrWhiteSpace(routePrefix)
            ? "kubejob"
            : routePrefix.Trim('/');
    }

    public void Apply(ControllerModel controller)
    {
        if (controller.ControllerType.Name != "DashboardController")
        {
            return;
        }

        foreach (var selector in controller.Selectors)
        {
            selector.AttributeRouteModel = selector.AttributeRouteModel is null
                ? new AttributeRouteModel { Template = _routePrefix }
                : AttributeRouteModel.CombineAttributeRouteModel(
                    new AttributeRouteModel { Template = _routePrefix },
                    selector.AttributeRouteModel);
        }
    }
}
