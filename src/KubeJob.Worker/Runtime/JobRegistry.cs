using System.Reflection;
using System.Text.Json;
using KubeJob.Core.Attributes;
using KubeJob.Core.Context;
using KubeJob.Core.Enums;
using KubeJob.Core.Interfaces;

namespace KubeJob.Worker.Runtime;

public sealed class JobRegistry
{
    private readonly Dictionary<string, JobDescriptor> _byName;
    private JobRegistry(Dictionary<string, JobDescriptor> byName) => _byName = byName;
    public IReadOnlyCollection<JobDescriptor> Jobs => _byName.Values;
    public bool TryGet(string name, out JobDescriptor descriptor) => _byName.TryGetValue(name, out descriptor!);

    public static JobRegistry Discover(IEnumerable<Assembly> assemblies, JsonSerializerOptions? jsonOptions = null)
    {
        var options = jsonOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var map = new Dictionary<string, JobDescriptor>(StringComparer.Ordinal);
        foreach (var assembly in assemblies.Distinct())
        {
            foreach (var type in GetLoadableTypes(assembly))
            {
                if (type.IsAbstract || type.IsInterface) continue;
                var jobAttribute = type.GetCustomAttribute<KubeJobAttribute>();
                if (jobAttribute is null) continue;
                var descriptor = CreateDescriptor(type, jobAttribute, options);
                if (!map.TryAdd(descriptor.Name, descriptor))
                    throw new InvalidOperationException($"Duplicate KubeJob name '{descriptor.Name}'.");
            }
        }
        if (map.Count == 0) throw new InvalidOperationException("No [KubeJob] handlers were discovered.");
        return new JobRegistry(map);
    }

    private static JobDescriptor CreateDescriptor(Type type, KubeJobAttribute job, JsonSerializerOptions options)
    {
        var payload = type.GetCustomAttribute<KubeJobPayloadAttribute>();
        var selectors = type.GetCustomAttributes<NodeSelectorAttribute>()
            .ToDictionary(static x => x.Key, static x => x.Value, StringComparer.Ordinal);
        Func<object, KubeJobContextV2, CancellationToken, ValueTask> invoker;
        if (typeof(IKubeJobV2).IsAssignableFrom(type))
            invoker = static (handler, context, token) => ((IKubeJobV2)handler).ExecuteAsync(context, token);
        else if (typeof(IKubeJob).IsAssignableFrom(type))
        {
            invoker = static (handler, context, token) => new ValueTask(((IKubeJob)handler).ExecuteAsync(
                new KubeJobContext
                {
                    RunId = context.RunId,
                    SpecId = context.SpecId,
                    BatchId = context.BatchId,
                    ShardIndex = context.ShardIndex,
                    TotalShards = context.TotalShards,
                    ServiceProvider = context.Services,
                    Logger = context.Logger
                }, token));
        }
        else
        {
            var typed = type.GetInterfaces().SingleOrDefault(static x =>
                x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IKubeJob<>));
            if (typed is null) throw new InvalidOperationException(
                $"{type.FullName} must implement IKubeJob, IKubeJobV2 or IKubeJob<TPayload>.");
            var factory = typeof(JobRegistry).GetMethod(nameof(CreateTypedInvoker), BindingFlags.NonPublic | BindingFlags.Static)!;
            invoker = (Func<object, KubeJobContextV2, CancellationToken, ValueTask>)factory
                .MakeGenericMethod(type, typed.GetGenericArguments()[0]).Invoke(null, new object[] { options })!;
        }
        return new JobDescriptor(job.Name, type, invoker, job.Cron, job.ExecuteModel,
            Math.Max(1, job.TotalShards), Math.Max(1, job.TimeoutSeconds), Math.Max(0, job.MaxRetries),
            payload?.HandlerVersion ?? type.Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? string.Empty,
            payload?.SchemaVersion ?? 1, selectors);
    }

    private static Func<object, KubeJobContextV2, CancellationToken, ValueTask> CreateTypedInvoker<TJob, TPayload>(
        JsonSerializerOptions options) where TJob : class, IKubeJob<TPayload>
    {
        return async (handler, context, token) =>
        {
            var payload = JsonSerializer.Deserialize<TPayload>(context.PayloadUtf8.Span, options);
            if (payload is null) throw new JsonException($"Payload for {typeof(TPayload).FullName} was null.");
            await ((TJob)handler).ExecuteAsync(payload, context, token);
        };
    }

    private static IEnumerable<Type> GetLoadableTypes(Assembly assembly)
    {
        try { return assembly.GetTypes(); }
        catch (ReflectionTypeLoadException ex) { return ex.Types.OfType<Type>(); }
    }
}

public sealed record JobDescriptor(
    string Name,
    Type HandlerType,
    Func<object, KubeJobContextV2, CancellationToken, ValueTask> InvokeAsync,
    string Cron,
    ExecuteModel ExecuteModel,
    int TotalShards,
    int TimeoutSeconds,
    int MaxRetries,
    string HandlerVersion,
    int PayloadSchemaVersion,
    IReadOnlyDictionary<string, string> NodeSelectors);
