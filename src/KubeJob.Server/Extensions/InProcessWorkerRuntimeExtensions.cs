using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KubeJob.Server.Extensions;

public static class InProcessWorkerRuntimeExtensions
{
    /// <summary>
    /// Routes the worker protocol directly to the configured runtime stores.
    /// Call this in unified applications to avoid localhost HTTP.
    /// </summary>
    public static IServiceCollection UseInProcessKubeJobWorkerTransport(
        this IServiceCollection services)
    {
        services.Replace(ServiceDescriptor.Singleton<IWorkerRuntimeClient, InProcessWorkerRuntimeClient>());
        return services;
    }
}
