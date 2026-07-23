using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace KubeJob.Server.Telemetry;

public static class KubeJobOpenTelemetryExtensions
{
    public static IServiceCollection AddKubeJobOpenTelemetry(
        this IServiceCollection services,
        Action<TracerProviderBuilder>? configureTracing = null,
        Action<MeterProviderBuilder>? configureMetrics = null)
    {
        services.AddOpenTelemetry()
            .WithTracing(builder =>
            {
                builder.AddSource(KubeJobTelemetry.ActivitySourceName);
                configureTracing?.Invoke(builder);
            })
            .WithMetrics(builder =>
            {
                builder.AddMeter(KubeJobTelemetry.MeterName);
                configureMetrics?.Invoke(builder);
            });
        return services;
    }
}
