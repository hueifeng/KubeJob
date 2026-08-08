using KubeJob.Core.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Resolves the execution authority for each logical Queue from deployment
/// configuration. Resolution is local and does not require a database read.
/// </summary>
public sealed class ConfigurationQueueRuntimeResolver : IQueueRuntimeResolver
{
    private readonly IOptionsMonitor<QueueRuntimeOptions> _options;

    public ConfigurationQueueRuntimeResolver(IOptionsMonitor<QueueRuntimeOptions> options)
    {
        _options = options;
    }

    public QueueRuntimeRoute Resolve(string logicalQueue)
        => _options.CurrentValue.Resolve(logicalQueue);
}
