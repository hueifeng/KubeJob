using KubeJob.Core.Client;
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

        services.AddHttpClient<IJobClient, HttpJobClient>(client =>
        {
            client.BaseAddress = EnsureTrailingSlash(controlPlaneEndpoint);
            client.Timeout = TimeSpan.FromSeconds(30);
        });
        return services;
    }

    private static Uri EnsureTrailingSlash(Uri endpoint)
    {
        var value = endpoint.AbsoluteUri;
        return value.EndsWith('/', StringComparison.Ordinal)
            ? endpoint
            : new Uri(value + '/', UriKind.Absolute);
    }
}
