using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Data;
using KubeJob.Server.Controllers;
using KubeJob.Server.Dashboard;
using KubeJob.Server.Options;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.ApplicationModels;
using Microsoft.AspNetCore.Mvc.Authorization;
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
        services.TryAddSingleton<IJobRuntimeMaintenanceStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<JobControlPlane>();
        services.TryAddSingleton<IJobMessageIngress, JobMessageIngress>();
        services.TryAddSingleton<WorkerControlPlane>();
        services.TryAddSingleton<ScheduleControlPlane>();
        services.TryAddSingleton<IWorkAvailableNotifier, PollingWorkAvailableNotifier>();
        services.TryAddSingleton<ICancelPublisher, NoopCancelPublisher>();
        services.AddOptions<QueueDeliveryOptions>();
        services.TryAddSingleton<IQueueRouter, ConfigurationQueueRouter>();
        services.TryAddSingleton<IExecutionGroupResolver, DefaultExecutionGroupResolver>();
        services.TryAddSingleton<IExecutionDispatcher, UnconfiguredExecutionDispatcher>();
        services.TryAddSingleton<IJobClient, DefaultJobClient>();
        services.TryAddSingleton<IJobScheduleClient, DefaultJobScheduleClient>();
        services.AddOptions<JobRuntimeOptions>();
        services.AddHealthChecks()
            .AddCheck<KubeJobRuntimeHealthCheck>(
                "kubejob-runtime",
                tags: new[] { "ready" });

        services.AddAuthorization();
        services.AddControllers(mvc =>
        {
            mvc.Conventions.Add(new KubeJobApiAuthorizationConvention(
                options.GetNormalizedClientAuthorizationPolicy(),
                options.GetNormalizedWorkerAuthorizationPolicy()));
        })
            .AddApplicationPart(typeof(JobsApiController).Assembly);
        services.AddHostedService<ScheduleReconcilerService>();
        services.AddHostedService<LeaseReaperService>();
        services.AddHostedService<OutboxPublisherService>();
        services.AddHostedService<RuntimeRetentionService>();
        return services;
    }

    /// <summary>
    /// Selects the broker-neutral work-signal publisher used by the transactional
    /// Outbox. Transport packages register their own implementation here.
    /// </summary>
    public static IServiceCollection UseKubeJobWorkAvailableNotifier<TNotifier>(
        this IServiceCollection services)
        where TNotifier : class, IWorkAvailableNotifier
    {
        services.Replace(ServiceDescriptor.Singleton<IWorkAvailableNotifier, TNotifier>());
        return services;
    }

    /// <summary>
    /// Configures deployment-level Queue routing. This is intentionally kept
    /// outside EnqueueJobRequest so business callers cannot select a physical
    /// execution mechanism for one Run.
    /// </summary>
    public static IServiceCollection ConfigureKubeJobQueueRouting(
        this IServiceCollection services,
        Action<QueueDeliveryOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        return services;
    }

    /// <summary>
    /// Registers the broker-specific execution dispatcher used by queues that
    /// are routed to BrokerDispatch.
    /// </summary>
    public static IServiceCollection UseKubeJobExecutionDispatcher<TDispatcher>(
        this IServiceCollection services)
        where TDispatcher : class, IExecutionDispatcher
    {
        services.Replace(ServiceDescriptor.Singleton<IExecutionDispatcher, TDispatcher>());
        return services;
    }

    /// <summary>
    /// Registers the broker-specific cancel publisher used by Direct Dispatch
    /// Mode to fanout per-group cancel signals to in-flight workers. MQ
    /// transport packages supply their own implementation.
    /// </summary>
    public static IServiceCollection UseKubeJobCancelPublisher<TCancelPublisher>(
        this IServiceCollection services)
        where TCancelPublisher : class, ICancelPublisher
    {
        services.Replace(ServiceDescriptor.Singleton<ICancelPublisher, TCancelPublisher>());
        return services;
    }

    public static IServiceCollection AddKubeJobDashboard(
        this IServiceCollection services,
        Action<KubeJobDashboardOptions>? configure = null)
    {
        var options = new KubeJobDashboardOptions();
        configure?.Invoke(options);

        services.AddSingleton(options);
        services.AddSingleton<DashboardCatalogReader>();
        services.AddAuthorization();
        services.AddControllersWithViews(mvc =>
        {
            mvc.Conventions.Add(new KubeJobDashboardRouteConvention(
                options.GetNormalizedRoutePrefix(),
                options.GetNormalizedAuthorizationPolicy()));
        })
        .AddApplicationPart(typeof(DashboardController).Assembly);
        return services;
    }

    public static IServiceCollection AddKubeJobDashboard(
        this IServiceCollection services,
        string routePrefix)
        => services.AddKubeJobDashboard(options => options.RoutePrefix = routePrefix);

    public static void InitializeKubeJobDatabase(this IApplicationBuilder app)
    {
        using var scope = app.ApplicationServices.CreateScope();
        var initializer = scope.ServiceProvider.GetRequiredService<IStorageInitializer>();
        initializer.Initialize();
    }
}

internal sealed class KubeJobApiAuthorizationConvention : IControllerModelConvention
{
    private readonly string? _clientPolicy;
    private readonly string? _workerPolicy;

    public KubeJobApiAuthorizationConvention(
        string? clientPolicy,
        string? workerPolicy)
    {
        _clientPolicy = clientPolicy;
        _workerPolicy = workerPolicy;
    }

    public void Apply(ControllerModel controller)
    {
        var controllerType = controller.ControllerType.AsType();
        var policy = controllerType == typeof(JobRuntimeController)
            ? _workerPolicy
            : controllerType == typeof(JobsApiController)
              || controllerType == typeof(SchedulesApiController)
              || controllerType == typeof(JobAttemptSnapshotsController)
                ? _clientPolicy
                : null;

        if (policy is not null)
        {
            controller.Filters.Add(new AuthorizeFilter(policy));
        }
    }
}

public sealed class KubeJobDashboardRouteConvention : IControllerModelConvention
{
    private readonly string _routePrefix;
    private readonly string? _authorizationPolicy;

    public KubeJobDashboardRouteConvention(
        string routePrefix,
        string? authorizationPolicy = null)
    {
        _routePrefix = string.IsNullOrWhiteSpace(routePrefix)
            ? "kubejob"
            : routePrefix.Trim('/');
        _authorizationPolicy = string.IsNullOrWhiteSpace(authorizationPolicy)
            ? null
            : authorizationPolicy.Trim();
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

        if (_authorizationPolicy is not null)
        {
            controller.Filters.Add(new AuthorizeFilter(_authorizationPolicy));
        }
    }
}
