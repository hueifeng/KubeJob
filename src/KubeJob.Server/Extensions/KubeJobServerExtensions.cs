using System;
using KubeJob.Core.Client;
using KubeJob.Server.Data;
using KubeJob.Server.Options;
using KubeJob.Server.Runtime;
using KubeJob.Server.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.ApplicationModels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Server.Extensions
{
    public static class KubeJobServerExtensions
    {
        public static IServiceCollection AddKubeJobServer(this IServiceCollection services, Action<KubeJobServerOptions>? configure = null)
        {
            var options = new KubeJobServerOptions();
            configure?.Invoke(options);

            services.AddSingleton<IServerIdentity, DefaultServerIdentity>();

            if (options.StorageConfigurator != null)
            {
                options.StorageConfigurator(services);
            }
            else
            {
                services.AddSingleton<IKubeJobRepository, InMemoryKubeJobRepository>();
            }

            // Runtime V2 is additive. Durable providers register these interfaces in
            // StorageConfigurator; otherwise the reference in-memory state machine is used.
            services.TryAddSingleton<InMemoryJobRuntimeStore>();
            services.TryAddSingleton<IJobSubmissionStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
            services.TryAddSingleton<IWorkerSessionStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
            services.TryAddSingleton<IJobClaimStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
            services.TryAddSingleton<IJobCompletionStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
            services.TryAddSingleton<IJobQueryStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
            services.TryAddSingleton<IOutboxStore>(sp => sp.GetRequiredService<InMemoryJobRuntimeStore>());
            services.TryAddSingleton<IWorkAvailableNotifier, PollingWorkAvailableNotifier>();
            services.TryAddSingleton<IJobClient, DefaultJobClient>();
            services.AddOptions<JobRuntimeOptions>();

            if (options.LockConfigurator != null)
            {
                options.LockConfigurator(services);
            }
            else
            {
                services.AddSingleton<IKubeJobLockProvider, InMemoryLockProvider>();
            }

            services.AddControllers();

            // Legacy runtime remains enabled during the migration window.
            services.AddHostedService<CronSchedulerService>();
            services.AddHostedService<JobDispatcherService>();
            services.AddHostedService<NodeHealthService>();
            services.AddHostedService<HistoryCleanupService>();

            // V2 reconcilers are bounded and do not require a leader process.
            services.AddHostedService<LeaseReaperService>();
            services.AddHostedService<OutboxPublisherService>();

            return services;
        }

        public static IServiceCollection AddKubeJobDashboard(this IServiceCollection services, string routePrefix = "kubejob")
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
            var init = scope.ServiceProvider.GetService<KubeJob.Server.Data.IStorageInitializer>();
            init?.Initialize();
        }
    }

    public class KubeJobDashboardRouteConvention : IControllerModelConvention
    {
        private readonly string _routePrefix;
        public KubeJobDashboardRouteConvention(string routePrefix)
        {
            _routePrefix = routePrefix.Trim('/');
        }

        public void Apply(ControllerModel controller)
        {
            if (controller.ControllerType.Name == "DashboardController")
            {
                foreach (var selector in controller.Selectors)
                {
                    if (selector.AttributeRouteModel != null)
                    {
                        selector.AttributeRouteModel = AttributeRouteModel.CombineAttributeRouteModel(
                            new AttributeRouteModel { Template = _routePrefix },
                            selector.AttributeRouteModel);
                    }
                    else
                    {
                        selector.AttributeRouteModel = new AttributeRouteModel { Template = _routePrefix };
                    }
                }
            }
        }
    }
}
