using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using KubeJob.Core.Runtime;
using KubeJob.Transport.RabbitMQ.Telemetry;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Publishes durable execution envelopes through a bounded pool of independent
/// RabbitMQ channels. RabbitMQ <c>IModel</c> instances are not thread-safe, so
/// each slot owns its connection, channel, and confirm/return registries.
/// </summary>
/// <remarks>
/// Publisher confirms are <b>pipelined</b>. <see cref="IModel.BasicPublish"/> runs
/// under the slot lock (microseconds), then the awaiter releases the lock and
/// waits for the broker's asynchronous <see cref="IModel.BasicAcks"/>/
/// <see cref="IModel.BasicNacks"/>/<see cref="IModel.BasicReturn"/> callback to
/// resolve a per-sequence-number <see cref="TaskCompletionSource{TResult}"/>. Many
/// envelopes can therefore be in flight per slot at once, amortizing the confirm
/// round trip instead of serializing every publish on it. The confirm/return
/// callbacks fire on RabbitMQ's I/O thread and only resolve TCSes / touch
/// concurrent dictionaries, so they never call back into the non-thread-safe
/// <c>IModel</c>.
/// </remarks>
public sealed class RabbitMqExecutionDispatcher : IExecutionTransport, IDisposable
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly RabbitMqExecutionOptions _options;
    private readonly PublisherSlot[] _slots;
    private readonly KubeJobRabbitMqMetrics? _metrics;
    private int _nextSlot;

    public RabbitMqExecutionDispatcher(
        IOptions<RabbitMqExecutionOptions> options,
        KubeJobRabbitMqMetrics? metrics = null)
    {
        _options = options.Value;
        _metrics = metrics;
        _options.Validate();
        _slots = Enumerable.Range(0, _options.PublisherConcurrency)
            .Select(_ => new PublisherSlot())
            .ToArray();
    }

    public string TransportId => _options.TransportId;

    /// <summary>
    /// Physical queue names for a logical queue on this transport: one dispatch
    /// queue per lane plus the group-shared retry and dead-letter queues.
    /// </summary>
    public IReadOnlyList<string> ResolvePhysicalQueueNames(string logicalQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalQueue);
        var names = new List<string>(_options.ExecutionLaneCount + 2);
        for (var lane = 0; lane < _options.ExecutionLaneCount; lane++)
        {
            names.Add(_options.GetConsumerQueueName(logicalQueue, lane));
        }

        names.Add(_options.GetSharedRetryQueueName());
        names.Add(_options.GetGroupDlqName());
        return names;
    }

    public async ValueTask PublishAsync(
        ExecutionEnvelope envelope,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(envelope);
        // The topology is provisioned for exactly one consumer group per
        // deployment; a group on the envelope that differs from this
        // deployment's group would be unroutable (no exchange/bindings exist).
        // Fail fast here so the outbox publisher surfaces a clear error instead
        // of a silent BasicReturn at the broker.
        if (!string.Equals(envelope.ConsumerGroup, _options.ConsumerGroup, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Execution envelope for Run '{envelope.RunId}' targets consumer group '{envelope.ConsumerGroup}', " +
                $"but this transport is provisioned for group '{_options.ConsumerGroup}'.");
        }

        // The lane is derived from the run's PartitionKey (ConcurrencyKey) so
        // same-key runs co-locate on one physical lane queue. N=1 collapses to
        // lane 0 and a bare routing key, byte-identical to the pre-lane publish.
        var lane = ExecutionLaneRouter.GetLane(envelope.PartitionKey, _options.ExecutionLaneCount);
        var routingKey = _options.GetLaneRoutingKey(envelope.Queue, lane);
        if (Encoding.UTF8.GetByteCount(routingKey) >= 255)
        {
            throw new InvalidOperationException(
                "RabbitMQ execution routing keys must be shorter than 255 UTF-8 bytes.");
        }

        var startedAt = _metrics?.IsPublishDurationEnabled == true
            ? Stopwatch.GetTimestamp()
            : 0L;
        var slotIndex = (int)((uint)Interlocked.Increment(ref _nextSlot) % (uint)_slots.Length);
        var slot = _slots[slotIndex];

        // Serialize outside the slot lock so CPU-intensive work does not widen
        // the critical section. Using SerializeToUtf8Bytes avoids the extra
        // string allocation from Serialize + GetBytes.
        var body = JsonSerializer.SerializeToUtf8Bytes(envelope, SerializerOptions);

        PendingPublish? pending = null;
        lock (slot.Gate)
        {
            try
            {
                var channel = EnsureChannel(slot);
                // The group exchange is declared once per channel in
                // EnsureChannel; per-publish declaration would be an extra
                // round trip on every envelope.
                var groupExchange = _options.GetGroupExchangeName(envelope.ConsumerGroup);
                var properties = channel.CreateBasicProperties();
                properties.ContentType = "application/json";
                properties.DeliveryMode = 2;
                properties.Type = "execution-envelope";
                properties.MessageId = envelope.EventId;

                // NextPublishSeqNo is the sequence number BasicPublish will use;
                // record it before publishing so the ack/nack callback can find
                // this envelope even if it arrives before BasicPublish returns.
                var seqNo = channel.NextPublishSeqNo;
                pending = new PendingPublish(seqNo, envelope.EventId);
                slot.BySeqNo[seqNo] = pending;
                slot.ByEventId[envelope.EventId] = pending;

                channel.BasicPublish(
                    exchange: groupExchange,
                    routingKey: routingKey,
                    mandatory: true,
                    basicProperties: properties,
                    body: body);
            }
            catch
            {
                if (pending is not null)
                {
                    AbandonPending(slot, pending.SeqNo, envelope.EventId);
                }

                _metrics?.PublishFailed();
                ResetConnection(slot);
                throw;
            }
        }

        // Await the broker confirm OUTSIDE the slot lock so the slot can keep
        // publishing while this envelope is still in flight.
        try
        {
            await pending.Tcs.Task
                .WaitAsync(_options.PublisherConfirmTimeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // The broker may still deliver the envelope; abandon the wait and
            // leave the durable outbox row for reconciliation/retry.
            AbandonPending(slot, pending.SeqNo, envelope.EventId);
            _metrics?.PublishFailed();
            throw new IOException(
                $"RabbitMQ did not confirm execution envelope '{envelope.EventId}' " +
                $"within {_options.PublisherConfirmTimeout}.");
        }
        catch (OperationCanceledException)
        {
            AbandonPending(slot, pending.SeqNo, envelope.EventId);
            _metrics?.PublishFailed();
            throw;
        }
        catch (IOException)
        {
            // Nack or unroutable return: the confirm callback already recorded
            // the failure on the pending TCS. Leave the channel open so sibling
            // in-flight publishes on this slot can still be confirmed.
            _metrics?.PublishFailed();
            throw;
        }

        _metrics?.Published(startedAt == 0
            ? TimeSpan.Zero
            : Stopwatch.GetElapsedTime(startedAt));
    }

    public void Dispose()
    {
        foreach (var slot in _slots)
        {
            lock (slot.Gate)
            {
                ResetConnection(slot);
            }
        }
    }

    private IModel EnsureChannel(PublisherSlot slot)
    {
        if (slot.Channel is { IsOpen: true })
        {
            return slot.Channel;
        }

        ResetConnection(slot);
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        slot.Connection = factory.CreateConnection("KubeJob.ExecutionDispatcher");
        var channel = slot.Connection.CreateModel();
        channel.ExchangeDeclare(
            exchange: _options.GetGroupExchangeName(),
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);
        channel.ConfirmSelect();

        // Register confirm/return callbacks once per channel. They resolve the
        // pending TCSes registered in PublishAsync and never touch the IModel.
        channel.BasicAcks += (_, args) => OnConfirm(slot, args.DeliveryTag, args.Multiple, nack: false);
        channel.BasicNacks += (_, args) => OnConfirm(slot, args.DeliveryTag, args.Multiple, nack: true);
        channel.BasicReturn += (_, args) => OnReturn(slot, args);

        slot.Channel = channel;
        return channel;
    }

    private static void OnConfirm(PublisherSlot slot, ulong deliveryTag, bool multiple, bool nack)
    {
        if (multiple)
        {
            // The broker batched confirms: resolve every in-flight sequence
            // number up to and including deliveryTag. Snapshot the keys first
            // because Resolve mutates the dictionary while we iterate.
            foreach (var seqNo in slot.BySeqNo.Keys)
            {
                if (seqNo <= deliveryTag
                    && slot.BySeqNo.TryGetValue(seqNo, out var pending))
                {
                    Resolve(slot, pending, nack);
                }
            }
        }
        else if (slot.BySeqNo.TryGetValue(deliveryTag, out var pending))
        {
            Resolve(slot, pending, nack);
        }
    }

    private static void OnReturn(PublisherSlot slot, BasicReturnEventArgs args)
    {
        var messageId = args.BasicProperties?.MessageId;
        if (string.IsNullOrEmpty(messageId))
        {
            return;
        }

        // BasicReturn carries the MessageId (not the publish sequence number),
        // so correlate via the by-event-id registry. The matching ack will fail
        // the publish instead of completing it.
        if (slot.ByEventId.TryGetValue(messageId!, out var pending))
        {
            pending.MarkReturned(args);
        }
    }

    private static void Resolve(PublisherSlot slot, PendingPublish pending, bool nack)
    {
        slot.BySeqNo.TryRemove(pending.SeqNo, out _);
        slot.ByEventId.TryRemove(pending.EventId, out _);

        if (nack)
        {
            pending.Tcs.TrySetException(new IOException(
                $"RabbitMQ nacked execution envelope '{pending.EventId}'."));
        }
        else if (pending.TryMarkReturned(out var returned))
        {
            pending.Tcs.TrySetException(new IOException(
                $"RabbitMQ could not route execution envelope '{pending.EventId}' " +
                $"with routing key '{returned!.RoutingKey}' " +
                $"(reply code {returned.ReplyCode}: {returned.ReplyText})."));
        }
        else
        {
            pending.Tcs.TrySetResult(true);
        }
    }

    private static void AbandonPending(PublisherSlot slot, ulong seqNo, string eventId)
    {
        slot.BySeqNo.TryRemove(seqNo, out _);
        slot.ByEventId.TryRemove(eventId, out _);
    }

    private static void ResetConnection(PublisherSlot slot)
    {
        // Fail every still-in-flight publish so its awaiter does not hang when
        // the channel it was waiting on is disposed.
        var error = new IOException("RabbitMQ publisher channel was reset.");
        foreach (var pending in slot.BySeqNo.Values)
        {
            pending.Tcs.TrySetException(error);
        }

        slot.BySeqNo.Clear();
        slot.ByEventId.Clear();

        try
        {
            slot.Channel?.Dispose();
        }
        catch
        {
        }

        try
        {
            slot.Connection?.Dispose();
        }
        catch
        {
        }

        slot.Channel = null;
        slot.Connection = null;
    }

    private sealed class PublisherSlot
    {
        public object Gate { get; } = new();
        public IConnection? Connection { get; set; }
        public IModel? Channel { get; set; }
        public ConcurrentDictionary<ulong, PendingPublish> BySeqNo { get; } = new();
        public ConcurrentDictionary<string, PendingPublish> ByEventId { get; } = new();
    }

    private sealed class PendingPublish
    {
        public ulong SeqNo { get; }
        public string EventId { get; }
        public TaskCompletionSource<bool> Tcs { get; }

        // Touched only by RabbitMQ's I/O thread (BasicReturn then BasicAcks are
        // delivered serialized on one channel), so no synchronization needed.
        private BasicReturnEventArgs? _returned;

        public PendingPublish(ulong seqNo, string eventId)
        {
            SeqNo = seqNo;
            EventId = eventId;
            Tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void MarkReturned(BasicReturnEventArgs args) => _returned = args;

        public bool TryMarkReturned(out BasicReturnEventArgs? args)
        {
            args = _returned;
            return args is not null;
        }
    }
}