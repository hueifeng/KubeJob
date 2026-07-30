using System.Diagnostics;
using System.Diagnostics.Metrics;
using KubeJob.Core.Telemetry;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.Transport.RabbitMQ.Telemetry;

public sealed class KubeJobRabbitMqMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _published;
    private readonly Counter<long> _failed;
    private readonly Histogram<double> _publishDuration;

    public KubeJobRabbitMqMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(
            KubeJobTelemetry.RabbitMqMeterName,
            typeof(KubeJobRabbitMqMetrics).Assembly.GetName().Version?.ToString());
        _published = _meter.CreateCounter<long>("kubejob.rabbitmq.execution.published", "{message}");
        _failed = _meter.CreateCounter<long>("kubejob.rabbitmq.execution.publish_failures", "{message}");
        _publishDuration = _meter.CreateHistogram<double>("kubejob.rabbitmq.execution.publish.duration", "s");
    }

    public bool IsPublishDurationEnabled => _publishDuration.Enabled;

    public void Published(TimeSpan duration)
    {
        if (_published.Enabled)
        {
            _published.Add(1);
        }
        if (_publishDuration.Enabled)
        {
            _publishDuration.Record(duration.TotalSeconds);
        }
    }

    public void PublishFailed()
    {
        if (_failed.Enabled)
        {
            _failed.Add(1);
        }
    }

    public void Dispose() => _meter.Dispose();
}
