using System.Text;
using System.Text.Json;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Publishes per-group cancel signals to a fanout exchange so workers in the
/// same <see cref="RabbitMqExecutionOptions.ConsumerGroup"/> can abort an
/// in-flight <see cref="ExecutionEnvelope"/> before it ACKs. Workers in
/// different consumer groups receive independent copies of the cancel signal
/// (broker fanout), so this publisher does not need per-group routing keys.
///
/// The signal is non-authoritative — it only triggers the worker's
/// in-flight attempt cancellation; the durable cancel state still lives in
/// PostgreSQL's <c>Kj2_JobRuns.CancelRequested</c> column and the lease
/// reaper remains the correctness fallback.
/// </summary>
public sealed class RabbitMqCancelPublisher : ICancelPublisher, IDisposable
{
    internal const string EventTypeHeader = "X-KubeJob-Event-Type";
    internal const string EventTypeCancel = "cancel";

    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly RabbitMqExecutionOptions _options;
    private readonly object _gate = new();
    private IConnection? _connection;
    private IModel? _channel;
    private string? _declaredGroup;

    public RabbitMqCancelPublisher(IOptions<RabbitMqExecutionOptions> options)
    {
        _options = options.Value;
        _options.Validate();
    }

    public ValueTask PublishAsync(
        string group,
        string runId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);

        lock (_gate)
        {
            try
            {
                var channel = EnsureChannel(group);
                var properties = channel.CreateBasicProperties();
                properties.ContentType = "application/json";
                properties.DeliveryMode = 2;
                properties.Type = EventTypeCancel;
                properties.Headers = new Dictionary<string, object>
                {
                    [EventTypeHeader] = EventTypeCancel
                };

                var body = Encoding.UTF8.GetBytes(
                    JsonSerializer.Serialize(new { runId }, SerializerOptions));

                channel.BasicPublish(
                    exchange: _options.GetCancelExchangeName(group),
                    routingKey: string.Empty,
                    mandatory: false,
                    basicProperties: properties,
                    body: body);

                if (!channel.WaitForConfirms(_options.PublisherConfirmTimeout))
                {
                    throw new IOException(
                        $"RabbitMQ did not confirm KubeJob cancel signal for run '{runId}' " +
                        $"within {_options.PublisherConfirmTimeout}.");
                }
            }
            catch
            {
                ResetConnection();
                throw;
            }
        }

        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        lock (_gate)
        {
            ResetConnection();
        }
    }

    private IModel EnsureChannel(string group)
    {
        if (_channel is { IsOpen: true } && string.Equals(_declaredGroup, group, StringComparison.Ordinal))
        {
            return _channel;
        }

        ResetConnection();
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        _connection = factory.CreateConnection("KubeJob.CancelPublisher");
        _channel = _connection.CreateModel();

        var exchangeName = _options.GetCancelExchangeName(group);
        _channel.ExchangeDeclare(
            exchange: exchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null);

        _channel.ConfirmSelect();
        _declaredGroup = group;
        return _channel;
    }

    private void ResetConnection()
    {
        try
        {
            _channel?.Dispose();
        }
        catch
        {
        }

        try
        {
            _connection?.Dispose();
        }
        catch
        {
        }

        _channel = null;
        _connection = null;
        _declaredGroup = null;
    }
}
