using System.Diagnostics;

namespace KubeJob.Core.Telemetry;

/// <summary>
/// Stable names for KubeJob diagnostic publishers. Hosts subscribe to these
/// names through <c>MeterProviderBuilder.AddMeter</c> and
/// <c>TracerProviderBuilder.AddSource</c>; KubeJob does not own exporters.
/// </summary>
public static class KubeJobTelemetry
{
    public const string ActivitySourceName = "KubeJob";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    public const string ControlPlaneMeterName = "KubeJob.ControlPlane";
    public const string WorkerMeterName = "KubeJob.Worker";
    public const string PostgreSqlMeterName = "KubeJob.Storage.PostgreSQL";
    public const string RabbitMqMeterName = "KubeJob.Transport.RabbitMQ";
}
