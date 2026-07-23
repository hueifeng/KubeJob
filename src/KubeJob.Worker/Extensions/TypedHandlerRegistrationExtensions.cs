using System.Reflection;
using KubeJob.Core.Attributes;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Worker.Extensions;

public static class TypedHandlerRegistrationExtensions
{
    /// <summary>
    /// Registers a typed handler using the stable identifier declared by its
    /// KubeJob attribute. The same attribute is consumed by the source generator.
    /// </summary>
    public static IServiceCollection AddKubeJobHandler<TJob, TPayload>(
        this IServiceCollection services)
        where TJob : class, IKubeJob<TPayload>
    {
        var attribute = typeof(TJob)
            .GetCustomAttributesData()
            .SingleOrDefault(data => data.AttributeType == typeof(KubeJobAttribute));

        if (attribute is null
            || attribute.ConstructorArguments.Count == 0
            || attribute.ConstructorArguments[0].Value is not string jobKey
            || string.IsNullOrWhiteSpace(jobKey))
        {
            throw new InvalidOperationException(
                $"Typed handler '{typeof(TJob).FullName}' must declare [KubeJob(\"stable.key\")].");
        }

        return services.AddKubeJobHandler<TJob, TPayload>(new JobKey<TPayload>(jobKey));
    }
}
