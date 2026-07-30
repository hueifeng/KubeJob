using System.Diagnostics.Metrics;
using KubeJob.Core.Telemetry;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.Storage.PostgreSQL.Telemetry;

public sealed class KubeJobPostgreSqlMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Histogram<double> _databaseGateWaitDuration;

    public KubeJobPostgreSqlMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(
            KubeJobTelemetry.PostgreSqlMeterName,
            typeof(KubeJobPostgreSqlMetrics).Assembly.GetName().Version?.ToString());
        _databaseGateWaitDuration = _meter.CreateHistogram<double>(
            "kubejob.storage.database_gate_wait.duration",
            unit: "s",
            description: "Time spent waiting for a KubeJob PostgreSQL operation permit.");
    }

    public bool IsDatabaseGateWaitEnabled => _databaseGateWaitDuration.Enabled;

    public void DatabaseGateWaited(TimeSpan duration)
    {
        if (_databaseGateWaitDuration.Enabled)
        {
            _databaseGateWaitDuration.Record(duration.TotalSeconds);
        }
    }

    public void Dispose() => _meter.Dispose();
}
