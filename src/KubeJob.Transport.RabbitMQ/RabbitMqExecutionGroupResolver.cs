using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Transport.RabbitMQ;

internal sealed class RabbitMqExecutionGroupResolver : IExecutionGroupResolver
{
    private readonly IOptions<RabbitMqExecutionOptions> _options;

    public RabbitMqExecutionGroupResolver(IOptions<RabbitMqExecutionOptions> options)
    {
        _options = options;
    }

    public string Resolve(string logicalQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        return _options.Value.ConsumerGroup;
    }
}
