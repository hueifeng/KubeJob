using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using KubeJob.Worker.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the typed pull worker. Remote workers use HTTP by default;
    /// unified hosts replace the transport with the in-process implementation.
    /// </summary>
    public static IServiceCollection AddKubeJobWorker(
        this IServiceCollection services,
        Action<KubeJobWorkerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.AddMetrics();
        services.TryAddSingleton<JobHandlerRegistry>();
        services.AddKubeJobWorkerClaimTrigger();
        services.TryAddSingleton<KubeJobWorkerMetrics>();
        services.TryAddSingleton<HttpWorkerRuntimeClient>();
        services.TryAddSingleton<IWorkerRuntimeClient>(sp =>
            sp.GetRequiredService<HttpWorkerRuntimeClient>());
        services.TryAddSingleton<WorkerRuntimeService>();
        services.AddHostedService(sp => sp.GetRequiredService<WorkerRuntimeService>());

        // Register the execution pipeline builder and build the pipeline
        // from the configured middleware types.
        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KubeJobWorkerOptions>>().Value;
            var builder = new JobExecutionPipelineBuilder();

            foreach (var middlewareType in options.ExecutionMiddleware)
            {
                if (!typeof(IJobExecutionMiddleware).IsAssignableFrom(middlewareType))
                {
                    throw new InvalidOperationException(
                        $"Execution middleware type '{middlewareType.FullName}' " +
                        $"must implement {typeof(IJobExecutionMiddleware).FullName}.");
                }

                // Register the middleware type in DI if not already registered.
                services.TryAddTransient(middlewareType);
                builder.Use(middlewareType);
            }

            return builder;
        });

        return services;
    }

    /// <summary>
    /// Adds the single broker-neutral wait seam shared by the worker claim loop
    /// and optional local transport listeners.
    /// </summary>
    public static IServiceCollection AddKubeJobWorkerClaimTrigger(
        this IServiceCollection services)
    {
        services.TryAddSingleton<WorkerClaimTrigger>();
        services.TryAddSingleton<IWorkerClaimTrigger>(
            sp => sp.GetRequiredService<WorkerClaimTrigger>());
        services.TryAddSingleton<IWorkerClaimTriggerSource>(
            sp => sp.GetRequiredService<WorkerClaimTrigger>());
        return services;
    }

    /// <summary>
    /// Registers a specific middleware type for execution pipeline usage.
    /// The type must implement <see cref="IJobExecutionMiddleware"/>.
    /// </summary>
    public static IServiceCollection AddKubeJobMiddleware<TMiddleware>(
        this IServiceCollection services)
        where TMiddleware : class, IJobExecutionMiddleware
    {
        services.TryAddTransient<TMiddleware>();
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
