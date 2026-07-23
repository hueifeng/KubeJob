using System.Reflection;
using KubeJob.Core.Attributes;
using KubeJob.Core.Domain;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Options;

namespace KubeJob.Core.Extensions;

public static class KubeJobClientExtensions
{
    public static Task<JobSubmissionResult> EnqueueAsync<TJob, TPayload>(
        this IKubeJobClient client,
        TPayload payload,
        JobEnqueueOptions? options = null,
        CancellationToken cancellationToken = default)
        where TJob : class, IKubeJob<TPayload>
    {
        ArgumentNullException.ThrowIfNull(client);
        options ??= new JobEnqueueOptions();
        options.PayloadSchemaVersion = Math.Max(options.PayloadSchemaVersion, Cache<TJob>.PayloadSchemaVersion);
        return client.EnqueueAsync(Cache<TJob>.Name, payload, options, cancellationToken);
    }

    private static class Cache<TJob>
    {
        private static readonly Type Type = typeof(TJob);
        private static readonly KubeJobAttribute? Job = Type.GetCustomAttribute<KubeJobAttribute>();
        private static readonly KubeJobPayloadAttribute? Payload = Type.GetCustomAttribute<KubeJobPayloadAttribute>();
        public static readonly string Name = Job?.Name ?? Type.Name;
        public static readonly int PayloadSchemaVersion = Payload?.SchemaVersion ?? 1;
    }
}
