using System;
using System.Linq;
using System.Reflection;
using KubeJob.Core.Attributes;
using KubeJob.Core.Interfaces;
using KubeJob.Worker.Options;
using KubeJob.Worker.Services;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Worker.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKubeJobWorker(this IServiceCollection services, Action<KubeJobWorkerOptions> configure)
        {
            services.Configure(configure);
            
            // Auto-discover and register IJobs in the entry assembly
            var entryAssembly = Assembly.GetEntryAssembly();
            if (entryAssembly != null)
            {
                var jobTypes = entryAssembly.GetTypes()
                    .Where(t => typeof(IKubeJob).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract);

                foreach (var type in jobTypes)
                {
                    // Register as scoped so each execution gets a fresh instance
                    services.AddScoped(type);
                }
            }

            services.AddHostedService<WorkerAgentService>();

            return services;
        }
    }
}
