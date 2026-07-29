using KubeJob.Server.Extensions;
using KubeJob.Server.Options;
using KubeJob.Worker.Extensions;
using KubeJob.Worker.Options;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob;

public static class UnifiedHostingExtensions
{
    /// <summary>
    /// Adds the control plane and worker to one process while preserving the
    /// same attempt, lease, fencing, retry, and scheduling semantics used by
    /// distributed deployments. No localhost HTTP is used.
    /// </summary>
    public static IServiceCollection AddKubeJob(
        this IServiceCollection services,
        Action<KubeJobServerOptions>? configureServer,
        Action<KubeJobWorkerOptions> configureWorker)
    {
        ArgumentNullException.ThrowIfNull(configureWorker);

        services.AddKubeJobServer(configureServer);
        services.UseInProcessKubeJobWorkerTransport();
        services.AddKubeJobWorker(configureWorker);

        // The control plane and Worker share one container here, so the
        // Outbox's work-available hint can pulse the Worker's claim trigger
        // directly instead of leaving Pull-mode Workers on polling alone.
        services.UseKubeJobWorkAvailableNotifier<InProcessWorkAvailableNotifier>();
        return services;
    }
}
