using KubeJob.Core.Runtime;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Worker.Extensions;

public static class WorkerRuntimeExtensions
{
    /// <summary>
    /// Registers the production V2 worker execution engine. Remote workers use
    /// HTTP by default; unified hosts can replace IWorkerRuntimeClient before
    /// or after this call with an in-process implementation.
    /// </summary>
    public static IServiceCollection AddKubeJobWorkerRuntime(
        this IServiceCollection services,
        Action<KubeJobWorkerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure<KubeJobWorkerOptions>(options =>
        {
            configure(options);
            options.EnableRuntimeV2 = true;
        });

        services.TryAddSingleton<JobHandlerRegistry>();
        services.TryAddSingleton<HttpWorkerRuntimeClient>();
        services.TryAddSingleton<IWorkerRuntimeClient>(sp =>
            sp.GetRequiredService<HttpWorkerRuntimeClient>());
        services.AddHostedService<WorkerRuntimeService>();
        return services;
    }
}
