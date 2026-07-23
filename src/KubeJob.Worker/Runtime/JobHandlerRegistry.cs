using System.Text.Json;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Worker.Runtime;

public interface IJobHandlerInvoker
{
    string JobKey { get; }

    Type PayloadType { get; }

    ValueTask InvokeAsync(
        IServiceProvider serviceProvider,
        string payloadJson,
        JobExecutionContext context,
        CancellationToken cancellationToken);
}

public sealed class JobHandlerInvoker<TJob, TPayload> : IJobHandlerInvoker
    where TJob : class, IKubeJob<TPayload>
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    public JobHandlerInvoker(string jobKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobKey);
        JobKey = jobKey;
    }

    public string JobKey { get; }

    public Type PayloadType => typeof(TPayload);

    public ValueTask InvokeAsync(
        IServiceProvider serviceProvider,
        string payloadJson,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.Deserialize<TPayload>(payloadJson, SerializerOptions);
        if (payload is null)
        {
            throw new JsonException($"Payload for job '{JobKey}' deserialized to null.");
        }

        var handler = serviceProvider.GetRequiredService<TJob>();
        return handler.ExecuteAsync(payload, context, cancellationToken);
    }
}

public sealed class JobHandlerRegistry
{
    private readonly IReadOnlyDictionary<string, IJobHandlerInvoker> _handlers;

    public JobHandlerRegistry(IEnumerable<IJobHandlerInvoker> handlers)
    {
        var map = new Dictionary<string, IJobHandlerInvoker>(StringComparer.Ordinal);
        foreach (var handler in handlers)
        {
            if (!map.TryAdd(handler.JobKey, handler))
            {
                throw new InvalidOperationException($"Duplicate KubeJob handler registration for '{handler.JobKey}'.");
            }
        }

        _handlers = map;
        Capabilities = map.Keys.OrderBy(x => x, StringComparer.Ordinal).ToArray();
    }

    public IReadOnlyList<string> Capabilities { get; }

    public bool TryGet(string jobKey, out IJobHandlerInvoker handler) =>
        _handlers.TryGetValue(jobKey, out handler!);
}
