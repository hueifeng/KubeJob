using KubeJob.ControlPlane.Runtime;
using KubeJob.ControlPlane.Telemetry;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Periodically refreshes the cached KeyOrdered ordering backlog snapshot that
/// backs the control-plane observable gauges. The snapshot is read from the
/// dashboard store on a fixed cadence and cached in the metrics object, so a
/// metrics scrape returns the cached value and never triggers a database query
/// or a table scan.
/// </summary>
internal sealed class OrderingMetricsRefreshService : BackgroundService
{
    private readonly IJobRuntimeDashboardStore _dashboard;
    private readonly KubeJobControlPlaneMetrics? _metrics;
    private readonly TimeSpan _refreshInterval;
    private readonly ILogger<OrderingMetricsRefreshService> _logger;

    public OrderingMetricsRefreshService(
        IJobRuntimeDashboardStore dashboard,
        IOptions<JobRuntimeOptions> options,
        KubeJobControlPlaneMetrics? metrics = null,
        ILogger<OrderingMetricsRefreshService>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(dashboard);
        ArgumentNullException.ThrowIfNull(options);
        _dashboard = dashboard;
        _metrics = metrics;
        _refreshInterval = options.Value.OrderingBacklogRefreshInterval;
        _logger = logger ?? NullLogger<OrderingMetricsRefreshService>.Instance;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Delay once before the first snapshot so the host is ready and the
        // first scrape does not race the refresh.
        try
        {
            await Task.Delay(_refreshInterval, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (_metrics is not null)
                {
                    var samples = await _dashboard.GetOrderingBacklogAsync(stoppingToken);
                    _metrics.UpdateOrderingBacklog(samples);
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                // A scrape must never fail because the refresh loop threw; the
                // cached value simply goes stale until the next iteration.
                _logger.LogWarning(ex, "Failed to refresh ordering backlog snapshot.");
            }

            try
            {
                await Task.Delay(_refreshInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}