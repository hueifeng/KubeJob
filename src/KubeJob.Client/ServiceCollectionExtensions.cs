using KubeJob.Core.Client;
using KubeJob.Core.Scheduling;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Client;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKubeJobClient(
        this IServiceCollection services,
        Uri controlPlaneEndpoint)
    {
        ArgumentNullException.ThrowIfNull(controlPlaneEndpoint);
        if (!controlPlaneEndpoint.IsAbsoluteUri)
        {
            throw new ArgumentException("The control-plane endpoint must be absolute.", nameof(controlPlaneEndpoint));
        }

        var endpoint = EnsureTrailingSlash(controlPlaneEndpoint);
        services.AddHttpClient<IJobClient, HttpJobClient>(client =>
        {
            client.BaseAddress = endpoint;
            client.Timeout = TimeSpan.FromSeconds(30);
        });
        services.AddHttpClient<IJobScheduleClient, HttpJobScheduleClient>(client =>
        {
            client.BaseAddress = endpoint;
            client.Timeout = TimeSpan.FromSeconds(30);
        });
        return services;
    }

    private static Uri EnsureTrailingSlash(Uri endpoint)
    {
        var value = endpoint.AbsoluteUri;
        return value.EndsWith("/", StringComparison.Ordinal)
            ? endpoint
            : new Uri(value + "/", UriKind.Absolute);
    }
}
