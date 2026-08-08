using KubeJob.Core.Client;
using KubeJob.Core.Events;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Core.Transport;
using KubeJob.Server.ControlPlane;
using KubeJob.ControlPlane.Telemetry;
using KubeJob.ControlPlane.Data;
using KubeJob.Server.Controllers;
using KubeJob.Server.Dashboard;
using KubeJob.Server.Options;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.ApplicationModels;
using Microsoft.AspNetCore.Mvc.Authorization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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

        services.AddMetrics();
        services.TryAddSingleton<InMemoryJobRuntimeStore>(sp =>
            new InMemoryJobRuntimeStore(
                sp.GetRequiredService<IWorkAvailableNotifier>() is not NoopWorkAvailableNotifier));
        services.TryAddSingleton<IJobSubmissionStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IWorkerSessionStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobClaimStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobCompletionStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobQueryStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobScheduleStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IOutboxStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobRuntimeDashboardStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<IJobRuntimeMaintenanceStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
        services.TryAddSingleton<KubeJobControlPlaneMetrics>();
        services.TryAddSingleton<OutboxPublisherSignal>(sp =>
            new OutboxPublisherSignal(
                sp.GetRequiredService<IWorkAvailableNotifier>() is not NoopWorkAvailableNotifier));
        services.TryAddSingleton<JobControlPlane>(sp => new JobControlPlane(
            sp.GetRequiredService<IJobSubmissionStore>(),
            sp.GetRequiredService<IJobQueryStore>(),
            sp.GetRequiredService<IQueueRouter>(),
            sp.GetRequiredService<IOptions<JobRuntimeOptions>>(),
            sp.GetRequiredService<OutboxPublisherSignal>(),
            sp.GetService<KubeJobControlPlaneMetrics>()));
        services.TryAddSingleton<JobMessageIngress>();
        services.TryAddSingleton<IJobMessageIngress>(sp => sp.GetRequiredService<JobMessageIngress>());
        services.TryAddSingleton<IJobMessageIngressBatch>(sp => sp.GetRequiredService<JobMessageIngress>());
        services.TryAddSingleton<CompletionBatcher>();
        services.TryAddSingleton<WorkerControlPlane>();
        services.TryAddSingleton<ScheduleControlPlane>();
        services.TryAddSingleton<IWorkAvailableNotifier, NoopWorkAvailableNotifier>();

        // PostgresManaged policy is always PostgreSQL-authoritative. These
        // settings only describe managed worker eligibility and ordering.
        services.AddOptions<QueueDeliveryOptions>();
        services.TryAddSingleton<QueueCatalog>();
        services.TryAddSingleton<IQueueRouter, ConfigurationQueueRouter>();

        // V3 Queue authority and Event Topic routing are local deployment
        // configuration. BrokerNative publish never reads PostgreSQL simply to
        // decide where a message belongs.
        services.AddOptions<QueueRuntimeOptions>();
        services.TryAddSingleton<IQueueRuntimeResolver, ConfigurationQueueRuntimeResolver>();
        services.AddOptions<EventRuntimeOptions>();
        services.TryAddSingleton<IMessageTransportRegistry, MessageTransportRegistry>();

        services.TryAddSingleton<QueueInventoryService>();
        services.TryAddSingleton<IJobClient, DefaultJobClient>();
        services.TryAddSingleton<IEventBus, DefaultEventBus>();
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
        services.AddHostedService<OrderingMetricsRefreshService>();
        var clientPolicy = options.GetNormalizedClientAuthorizationPolicy();
        var workerPolicy = options.GetNormalizedWorkerAuthorizationPolicy();
        services.AddHostedService(sp => new KubeJobAuthorizationPolicyWarningService(
            new[] { ("client", clientPolicy), ("worker", workerPolicy) },
            sp.GetService<ILogger<KubeJobAuthorizationPolicyWarningService>>()
                ?? NullLogger<KubeJobAuthorizationPolicyWarningService>.Instance));
        return services;
    }

    public static IServiceCollection UseKubeJobWorkAvailableNotifier<TNotifier>(
        this IServiceCollection services)
        where TNotifier : class, IWorkAvailableNotifier
    {
        services.Replace(ServiceDescriptor.Singleton<IWorkAvailableNotifier, TNotifier>());
        return services;
    }

    /// <summary>
    /// Configures PostgresManaged worker eligibility and ordering policy. This
    /// API never selects a broker or changes execution authority.
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
    /// Configures the single execution authority of each logical Job Queue.
    /// </summary>
    public static IServiceCollection ConfigureKubeJobQueueRuntimes(
        this IServiceCollection services,
        Action<QueueRuntimeOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        return services;
    }

    /// <summary>
    /// Maps logical Event Topics to installed transport adapters.
    /// </summary>
    public static IServiceCollection ConfigureKubeJobEventRuntimes(
        this IServiceCollection services,
        Action<EventRuntimeOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
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
        var dashboardPolicy = options.GetNormalizedAuthorizationPolicy();
        services.AddHostedService(sp => new KubeJobAuthorizationPolicyWarningService(
            new[] { ("dashboard", dashboardPolicy) },
            sp.GetService<ILogger<KubeJobAuthorizationPolicyWarningService>>()
                ?? NullLogger<KubeJobAuthorizationPolicyWarningService>.Instance));
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

internal sealed class KubeJobAuthorizationPolicyWarningService : IHostedService
{
    private readonly (string Surface, string? Policy)[] _surfaces;
    private readonly ILogger<KubeJobAuthorizationPolicyWarningService> _logger;

    public KubeJobAuthorizationPolicyWarningService(
        (string Surface, string? Policy)[] surfaces,
        ILogger<KubeJobAuthorizationPolicyWarningService> logger)
    {
        _surfaces = surfaces;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var (surface, policy) in _surfaces)
        {
            if (policy is null)
            {
                _logger.LogWarning(
                    "KubeJob {Surface} endpoints have no authorization policy configured; " +
                    "they are reachable anonymously. Configure the corresponding policy option " +
                    "before deploying to production.",
                    surface);
            }
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
