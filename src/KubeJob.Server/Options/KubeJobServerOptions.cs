using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Server.Options;

/// <summary>
/// Configuration options for the KubeJob V2 control plane.
/// </summary>
public sealed class KubeJobServerOptions
{
    /// <summary>
    /// Storage providers replace the reference in-memory state machine by
    /// registering the V2 runtime store interfaces.
    /// </summary>
    public Action<IServiceCollection>? StorageConfigurator { get; set; }

    /// <summary>
    /// Uses the reference in-memory V2 state machine. This is already the
    /// default and is intended for tests, samples, and single-process hosts.
    /// </summary>
    public KubeJobServerOptions UseInMemory()
    {
        StorageConfigurator = null;
        return this;
    }
}
