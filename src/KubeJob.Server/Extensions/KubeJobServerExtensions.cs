using System;
using KubeJob.Server.Data;
using KubeJob.Server.Options;
using KubeJob.Server.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.ApplicationModels;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Server.Extensions
{
    public static class KubeJobServerExtensions
    {
        public static IServiceCollection AddKubeJobServer(this IServiceCollection services, Action<KubeJobServerOptions>? configure = null)
        {
            var options = new KubeJobServerOptions();
            configure?.Invoke(options);
            services.AddSingleton(options);

            services.AddSingleton<IServerIdentity, DefaultServerIdentity>();

            if (options.StorageConfigurator != null)
            {
                options.StorageConfigurator(services);
            }
            else
            {
                // Default fallback if not configured
                services.AddSingleton<IKubeJobRepository, InMemoryKubeJobRepository>();
            }

            if (options.LockConfigurator != null)
            {
                options.LockConfigurator(services);
            }
            else
            {
                // Default fallback if not configured
                services.AddSingleton<IKubeJobLockProvider, InMemoryLockProvider>();
            }

            services.AddHostedService<CronSchedulerService>();
            services.AddHostedService<JobDispatcherService>();
            services.AddHostedService<NodeHealthService>();
            services.AddHostedService<HistoryCleanupService>();

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
            // Use reflection or specific interface if you want an Initialize method for all storages
            // Here we just look for DbInitializer if it's Postgres
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
