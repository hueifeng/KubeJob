using System;
using System.Linq;
using System.Reflection;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using KubeJob.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Worker.Extensions
{
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Registers the legacy worker protocol for compatibility.
        /// </summary>
        public static IServiceCollection AddKubeJobWorker(this IServiceCollection services, Action<KubeJobWorkerOptions> configure)
        {
            services.Configure(configure);

            var entryAssembly = Assembly.GetEntryAssembly();
            if (entryAssembly != null)
            {
                var jobTypes = entryAssembly.GetTypes()
                    .Where(t => typeof(IKubeJob).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract);

                foreach (var type in jobTypes)
                {
                    services.AddScoped(type);
                }
            }

            services.AddHostedService<WorkerAgentService>();
            return services;
        }

        /// <summary>
        /// Registers the bounded pull/attempt/lease worker runtime.
        /// </summary>
        public static IServiceCollection AddKubeJobWorkerV2(
            this IServiceCollection services,
            Action<KubeJobWorkerOptions> configure)
        {
            services.Configure<KubeJobWorkerOptions>(options =>
            {
                configure(options);
                options.EnableRuntimeV2 = true;
            });

            services.TryAddSingleton<JobHandlerRegistry>();
            services.AddHostedService<WorkerRuntimeV2Service>();
            return services;
        }

        /// <summary>
        /// Registers a typed handler under a stable job key.
        /// </summary>
        public static IServiceCollection AddKubeJobHandler<TJob, TPayload>(
            this IServiceCollection services,
            JobKey<TPayload> jobKey)
            where TJob : class, IKubeJob<TPayload>
        {
            if (jobKey.IsEmpty)
            {
                throw new ArgumentException("The job key must be initialized.", nameof(jobKey));
            }

            services.AddScoped<TJob>();
            services.AddSingleton<IJobHandlerInvoker>(
                new JobHandlerInvoker<TJob, TPayload>(jobKey.Value));
            services.TryAddSingleton<JobHandlerRegistry>();
            return services;
        }
    }
}
