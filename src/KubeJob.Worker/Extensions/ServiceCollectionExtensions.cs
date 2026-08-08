using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using KubeJob.Worker.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the PostgreSQL/control-plane managed worker runtime. Remote
    /// workers use HTTP by default; unified hosts replace the transport with
    /// the in-process implementation.
    /// </summary>
    public static IServiceCollection AddKubeJobWorker(
        this IServiceCollection services,
        Action<KubeJobWorkerOptions> configure)
    {
        AddExecutionCore(services, configure);
        services.AddKubeJobWorkerClaimTrigger();
        services.TryAddSingleton<HttpWorkerRuntimeClient>();
        services.TryAddSingleton<IWorkerRuntimeClient>(sp =>
            sp.GetRequiredService<HttpWorkerRuntimeClient>());
        services.TryAddSingleton<WorkerRuntimeService>();
        services.AddHostedService(sp => sp.GetRequiredService<WorkerRuntimeService>());
        return services;
    }

    /// <summary>
    /// Registers only the transport-neutral execution core required by a
    /// BrokerNative worker. No control-plane runtime client, Claim loop,
    /// WorkerSession, lease renewal, or Managed hosted service is registered.
    /// A broker adapter (RabbitMQ, Kafka, etc.) is expected to own delivery,
    /// ACK/redelivery, retry, and DLQ semantics.
    /// </summary>
    public static IServiceCollection AddKubeJobBrokerNativeWorker(
        this IServiceCollection services,
        Action<KubeJobWorkerOptions> configure)
    {
        AddExecutionCore(services, configure);
        return services;
    }

    private static void AddExecutionCore(
        IServiceCollection services,
        Action<KubeJobWorkerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.AddMetrics();
        services.TryAddSingleton<JobHandlerRegistry>();
        services.TryAddSingleton<KubeJobWorkerMetrics>();

        // Register the execution pipeline builder and build the pipeline from
        // the configured middleware types. This is shared by Managed and
        // BrokerNative runtimes so business handler behavior stays identical.
        services.TryAddSingleton<JobExecutionPipelineBuilder>(sp =>
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

                // Preserve the existing registration behavior. Applications
                // may also register middleware explicitly with
                // AddKubeJobMiddleware<TMiddleware>().
                services.TryAddTransient(middlewareType);
                builder.Use(middlewareType);
            }

            return builder;
        });

        services.TryAddSingleton<IWorkerExecutionEngine>(sp =>
            new WorkerExecutionEngine(
                sp.GetRequiredService<IServiceScopeFactory>(),
                sp.GetRequiredService<JobHandlerRegistry>(),
                sp.GetRequiredService<ILogger<WorkerExecutionEngine>>(),
                sp.GetService<KubeJobWorkerMetrics>(),
                sp.GetService<JobExecutionPipelineBuilder>()));
        services.TryAddSingleton<BrokerNativeJobProcessor>();
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
