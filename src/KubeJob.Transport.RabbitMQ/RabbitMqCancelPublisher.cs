using System.Collections.Concurrent;
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
///
/// Each group owns its own channel because fanout exchanges are bound at
/// declare time; sharing one channel across groups would race when two
/// cancels for different groups land back-to-back.
/// </summary>
public sealed class RabbitMqCancelPublisher : ICancelPublisher, IDisposable
{
    internal const string EventTypeHeader = "X-KubeJob-Event-Type";
    internal const string EventTypeCancel = "cancel";

    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly RabbitMqExecutionOptions _options;
    private readonly ConcurrentDictionary<string, GroupChannel> _channels = new(StringComparer.Ordinal);
    private int _disposed;

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

        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(RabbitMqCancelPublisher));
        }

        var channel = GetOrCreateChannel(group);
        lock (channel.Gate)
        {
            try
            {
                var properties = channel.Model.CreateBasicProperties();
                properties.ContentType = "application/json";
                properties.DeliveryMode = 2;
                properties.Type = EventTypeCancel;
                properties.Headers = new Dictionary<string, object>
                {
                    [EventTypeHeader] = EventTypeCancel
                };

                var body = Encoding.UTF8.GetBytes(
                    JsonSerializer.Serialize(new { runId }, SerializerOptions));

                channel.Model.BasicPublish(
                    exchange: _options.GetCancelExchangeName(group),
                    routingKey: string.Empty,
                    mandatory: false,
                    basicProperties: properties,
                    body: body);

                if (!channel.Model.WaitForConfirms(_options.PublisherConfirmTimeout))
                {
                    throw new IOException(
                        $"RabbitMQ did not confirm KubeJob cancel signal for run '{runId}' " +
                        $"within {_options.PublisherConfirmTimeout}.");
                }
            }
            catch
            {
                DisposeChannel(group, channel);
                throw;
            }
        }

        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var entry in _channels)
        {
            DisposeChannel(entry.Key, entry.Value);
        }
        _channels.Clear();
    }

    private GroupChannel GetOrCreateChannel(string group)
    {
        if (_channels.TryGetValue(group, out var existing) && existing.IsOpen)
        {
            return existing;
        }

        // Remove any stale entry so concurrent creators race on a fresh slot.
        _channels.TryRemove(group, out _);

        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        var connection = factory.CreateConnection("KubeJob.CancelPublisher");
        var model = connection.CreateModel();

        var exchangeName = _options.GetCancelExchangeName(group);
        model.ExchangeDeclare(
            exchange: exchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null);

        model.ConfirmSelect();

        var channel = new GroupChannel(connection, model);
        return _channels.GetOrAdd(group, channel);
    }

    private void DisposeChannel(string group, GroupChannel channel)
    {
        _channels.TryRemove(group, out _);
        channel.Dispose();
    }

    private sealed class GroupChannel : IDisposable
    {
        public GroupChannel(IConnection connection, IModel model)
        {
            Connection = connection;
            Model = model;
            Gate = new object();
        }

        public IConnection Connection { get; }
        public IModel Model { get; }
        public object Gate { get; }
        public bool IsOpen => Model.IsOpen && Connection.IsOpen;

        public void Dispose()
        {
            try
            {
                Model?.Dispose();
            }
            catch
            {
            }

            try
            {
                Connection?.Dispose();
            }
            catch
            {
            }
        }
    }
}
