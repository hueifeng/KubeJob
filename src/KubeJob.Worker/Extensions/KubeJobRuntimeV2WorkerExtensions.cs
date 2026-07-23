using System.Net;
using System.Reflection;
using System.Text.Json;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Extensions;

public static class KubeJobRuntimeV2WorkerExtensions
{
    public static IServiceCollection AddKubeJobWorkerV2(this IServiceCollection services,
        Action<KubeJobRuntimeV2WorkerOptions> configure,
        JsonSerializerOptions? payloadJsonOptions = null,
        params Assembly[] jobAssemblies)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.TryAddSingleton(TimeProvider.System);
        var assemblies = jobAssemblies.Length == 0
            ? new[] { Assembly.GetEntryAssembly() ?? typeof(KubeJobRuntimeV2WorkerExtensions).Assembly }
            : jobAssemblies;
        var registry = JobRegistry.Discover(assemblies, payloadJsonOptions);
        services.AddSingleton(registry);
        foreach (var job in registry.Jobs) services.TryAddScoped(job.HandlerType);

        services.AddHttpClient<IKubeJobRuntimeClient, HttpKubeJobRuntimeClient>((provider, client) =>
        {
            var options = provider.GetRequiredService<IOptions<KubeJobRuntimeV2WorkerOptions>>().Value;
            client.BaseAddress = new Uri(options.ServerEndpoint.EndsWith('/') ? options.ServerEndpoint : options.ServerEndpoint + "/");
            client.Timeout = options.RequestTimeout;
            client.DefaultRequestVersion = HttpVersion.Version20;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower;
        }).SetHandlerLifetime(Timeout.InfiniteTimeSpan)
          .ConfigurePrimaryHttpMessageHandler(static () => new SocketsHttpHandler
          {
              AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
              PooledConnectionLifetime = TimeSpan.FromMinutes(10),
              PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
              MaxConnectionsPerServer = 256,
              EnableMultipleHttp2Connections = true
          });
        services.AddHostedService<WorkerRuntimeV2Service>();
        return services;
    }
}
