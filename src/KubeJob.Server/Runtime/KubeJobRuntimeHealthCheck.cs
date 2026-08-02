using KubeJob.ControlPlane.Runtime;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Lightweight readiness probe for the configured runtime store. It performs
/// a primary-key lookup rather than a Dashboard aggregation, so the probe does
/// not scale with the number of historical Runs.
/// </summary>
public sealed class KubeJobRuntimeHealthCheck : IHealthCheck
{
    private const string ProbeRunId = "__kubejob_health_probe__";

    private readonly IJobQueryStore _queries;

    public KubeJobRuntimeHealthCheck(IJobQueryStore queries)
    {
        _queries = queries;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _queries.GetRunAsync(ProbeRunId, cancellationToken);
            return HealthCheckResult.Healthy("KubeJob runtime store is reachable.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            return HealthCheckResult.Unhealthy(
                "KubeJob runtime store is unavailable.",
                exception);
        }
    }
}
