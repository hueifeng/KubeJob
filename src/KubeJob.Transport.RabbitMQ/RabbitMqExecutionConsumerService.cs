using System.Text.Json;
using System.Threading.Channels;
using KubeJob.Core.Runtime;
using KubeJob.Transport.RabbitMQ.Telemetry;
using KubeJob.Worker.Options;
using KubeJob.Worker.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Consumes durable Execution Envelopes and delegates the whole execution
/// lifecycle to WorkerRuntimeService. ACK is emitted only after the worker has
/// durably completed or explicitly classified the Run as terminal/rejected.
///
/// Deliveries are collected into a bounded pending buffer and admitted to the
/// control plane in batches (see <see cref="RabbitMqExecutionOptions.AdmissionBatchSize"/>)
/// so per-envelope admission round trips amortize into roughly two database
/// transactions per batch. Per-envelope ACK/reject/retry semantics are
/// unchanged: the broker never learns about an envelope before its Run is
/// durably terminal.
///
/// All channels are passed by parameter rather than held as fields: the
/// reconnect loop creates fresh connections and channels per session, and a
/// delivery tag is only valid on the channel that delivered it.
/// </summary>
public sealed class RabbitMqExecutionConsumerService : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    /// <summary>Cancel markers use this AMQP <c>Type</c>; see message-transport.md.</summary>
    private const string CancelMarkerType = "cancel";

    private readonly RabbitMqExecutionOptions _options;
    private readonly KubeJobWorkerOptions _worker;
    private readonly WorkerRuntimeService _runtime;
    private readonly IWorkerRuntimeClient _runtimeClient;
    private readonly RabbitMqTopologyProvisioner _topology;
    private readonly ILogger<RabbitMqExecutionConsumerService> _logger;
    private readonly KubeJobRabbitMqMetrics? _metrics;

    public RabbitMqExecutionConsumerService(
        IOptions<RabbitMqExecutionOptions> options,
        IOptions<KubeJobWorkerOptions> worker,
        WorkerRuntimeService runtime,
        IWorkerRuntimeClient runtimeClient,
        RabbitMqTopologyProvisioner topology,
        ILogger<RabbitMqExecutionConsumerService> logger,
        KubeJobRabbitMqMetrics? metrics = null)
    {
        _options = options.Value;
        _worker = worker.Value;
        _runtime = runtime;
        _runtimeClient = runtimeClient;
        _topology = topology;
        _logger = logger;
        _metrics = metrics;
        _options.Validate();
        _worker.Validate();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeUntilStoppedAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (RabbitMqTopologyMismatchException exception)
            {
                // A cross-host option mismatch will never fix itself by
                // reconnecting; fail the hosted service so the supervisor can
                // restart with corrected configuration instead of spinning.
                _logger.LogError(
                    exception,
                    "RabbitMQ KubeJob execution consumer cannot start for worker {WorkerId}: broker topology does not match this deployment's configuration",
                    _worker.WorkerId);
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "RabbitMQ KubeJob execution consumer disconnected for worker {WorkerId}",
                    _worker.WorkerId);
                await Task.Delay(_options.ReconnectDelay, stoppingToken);
            }
        }
    }

    private async Task ConsumeUntilStoppedAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
            DispatchConsumersAsync = true,
            ConsumerDispatchConcurrency = _options.ConsumerDispatchConcurrency == 0
                ? Math.Max(1, _worker.MaxConcurrentJobs)
                : _options.ConsumerDispatchConcurrency,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection($"KubeJob.Execution.{_worker.WorkerId}");
        using var channel = connection.CreateModel();
        using var connectionLifetime = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        channel.BasicQos(0, _options.PrefetchCount, global: false);

        // The consumer channel never publishes, so it needs no confirms. All
        // retry publications go to a dedicated confirm channel so a blocked
        // publisher confirm can never stall delivery processing or ACKs.
        using var publishChannel = connection.CreateModel();
        publishChannel.ConfirmSelect();

        var channelGate = new object();
        // The publish channel is shared between the batch loop and the
        // fire-and-forget completion path. RabbitMQ.Client's IModel is not
        // thread-safe and WaitForConfirms tracks per-channel aggregate state,
        // so every publish-confirm-return sequence is serialized through this
        // gate.
        var publishChannelGate = new object();
        // Re-declare the topology on every consumer session. The separate
        // startup provisioner cannot repair a queue/exchange deleted while the
        // process is running; active declaration here makes reconnect self-heal
        // missing topology while RabbitMQ still rejects argument mismatches.
        var consumerQueues = new List<string>();
        for (var lane = 0; lane < _options.ExecutionLaneCount; lane++)
        {
            foreach (var logicalQueue in _worker.Queues)
            {
                consumerQueues.Add(_options.GetConsumerQueueName(logicalQueue, lane));
            }
        }

        consumerQueues = consumerQueues.Distinct(StringComparer.Ordinal).ToList();
        _topology.DeclareTopology(channel);

        var disconnected = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        void SignalDisconnected()
        {
            connectionLifetime.Cancel();
            disconnected.TrySetResult();
        }

        // Bounded pending buffer: at most one full admission batch waits for
        // the next drain, so local memory is independent of broker prefetch.
        var pending = Channel.CreateBounded<BasicDeliverEventArgs>(
            new BoundedChannelOptions(_options.AdmissionBatchSize)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
        var batchLoop = Task.Run(
            () => ProcessBatchesAsync(
                pending.Reader,
                channel,
                channelGate,
                publishChannel,
                publishChannelGate,
                connectionLifetime.Token),
            CancellationToken.None);

        foreach (var consumerQueue in consumerQueues)
        {
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ConsumerCancelled += (_, _) =>
            {
                SignalDisconnected();
                return Task.CompletedTask;
            };
            consumer.Received += (_, delivery) => OnDispatchDeliveryAsync(
                pending.Writer,
                channel,
                channelGate,
                delivery,
                connectionLifetime.Token);
            channel.BasicConsume(
                queue: consumerQueue,
                autoAck: false,
                consumer: consumer);
        }

        if (_options.EnableCancelQueue)
        {
            // The cancel queue name is keyed by the stable WorkerId, not the
            // per-restart SessionId, so a restart reuses the same queue name
            // instead of churning a new ephemeral queue in the management UI.
            // autoDelete removes it when the worker disconnects, so retired
            // workers do not accumulate queues. During an overlapping drain
            // (old and new session alive briefly), both sessions share the
            // queue and markers may land on either; the durable cancel row,
            // admission check, and renewal loop remain the correctness path.
            var cancelQueue = _options.GetCancelQueueName(
                _options.ConsumerGroup,
                _worker.WorkerId);
            channel.QueueDeclare(
                queue: cancelQueue,
                durable: false,
                exclusive: false,
                autoDelete: true,
                arguments: null);
            channel.QueueBind(
                queue: cancelQueue,
                exchange: _options.GetCancelExchangeName(_options.ConsumerGroup),
                routingKey: string.Empty,
                arguments: null);
            var cancelConsumer = new AsyncEventingBasicConsumer(channel);
            cancelConsumer.ConsumerCancelled += (_, _) =>
            {
                SignalDisconnected();
                return Task.CompletedTask;
            };
            cancelConsumer.Received += (_, delivery) => ProcessCancelDeliveryAsync(
                channel,
                channelGate,
                delivery);
            channel.BasicConsume(
                queue: cancelQueue,
                autoAck: false,
                consumer: cancelConsumer);
        }

        _logger.LogInformation(
            "RabbitMQ KubeJob execution consumer active for worker {WorkerId}, group {ConsumerGroup}, and queues {Queues}",
            _worker.WorkerId,
            _options.ConsumerGroup,
            string.Join(",", _worker.Queues));

        connection.ConnectionShutdown += (_, _) =>
        {
            SignalDisconnected();
        };
        connection.CallbackException += (_, _) =>
        {
            SignalDisconnected();
        };
        channel.ModelShutdown += (_, _) =>
        {
            SignalDisconnected();
        };
        await Task.WhenAny(
            disconnected.Task,
            Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken));
        connectionLifetime.Cancel();
        pending.Writer.TryComplete();
        try
        {
            await batchLoop.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested || connectionLifetime.IsCancellationRequested)
        {
        }
        stoppingToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Routes a delivery from a dispatch queue. Cancel markers that were
    /// mis-routed onto a dispatch queue are handled as cancel signals (the
    /// transport dispatches on <c>properties.Type</c>, never on body parsing);
    /// everything else enters the bounded batch buffer.
    /// </summary>
    private async Task OnDispatchDeliveryAsync(
        ChannelWriter<BasicDeliverEventArgs> pendingWriter,
        IModel channel,
        object channelGate,
        BasicDeliverEventArgs delivery,
        CancellationToken cancellationToken)
    {
        if (string.Equals(
                delivery.BasicProperties?.Type,
                CancelMarkerType,
                StringComparison.Ordinal))
        {
            await ProcessCancelDeliveryAsync(channel, channelGate, delivery);
            return;
        }

        try
        {
            // RabbitMQ.Client's Body memory is backed by a receive buffer and
            // is only valid during the ReceivedAsync callback. The batch loop
            // runs after this callback returns, so retain an owned copy instead
            // of enqueueing the broker-owned memory.
            var buffered = new BasicDeliverEventArgs(
                delivery.ConsumerTag,
                delivery.DeliveryTag,
                delivery.Redelivered,
                delivery.Exchange,
                delivery.RoutingKey,
                delivery.BasicProperties,
                delivery.Body.ToArray());
            await pendingWriter.WriteAsync(buffered, cancellationToken);
        }
        catch (ChannelClosedException)
        {
            // The consumer is shutting down; the delivery stays unacked and
            // the broker requeues it when the connection closes.
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    /// <summary>
    /// Drains the pending buffer into admission batches and processes each
    /// batch. Only this loop calls the batch admission path, so the control
    /// plane sees one claim transaction per batch rather than one per
    /// envelope.
    /// </summary>
    private async Task ProcessBatchesAsync(
        ChannelReader<BasicDeliverEventArgs> pendingReader,
        IModel channel,
        object channelGate,
        IModel publishChannel,
        object publishChannelGate,
        CancellationToken cancellationToken)
    {
        var batch = new List<BasicDeliverEventArgs>(_options.AdmissionBatchSize);
        try
        {
            while (await pendingReader.WaitToReadAsync(cancellationToken))
            {
                batch.Clear();
                var dispositioned = new HashSet<ulong>();
                while (batch.Count < _options.AdmissionBatchSize
                       && pendingReader.TryRead(out var delivery))
                {
                    batch.Add(delivery);
                }

                try
                {
                    await ProcessBatchAsync(
                        batch,
                        channel,
                        channelGate,
                        publishChannel,
                        publishChannelGate,
                        dispositioned,
                        cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Deliveries of the interrupted batch stay unacked and are
                    // requeued by the broker when the connection closes.
                    return;
                }
                catch (Exception exception)
                {
                    // Admission failed (control plane outage, DB failure, HTTP
                    // timeout). Leaving the batch unacked on a live channel
                    // would wedge the consumer forever once unacked deliveries
                    // saturate the prefetch limit, so republish every delivery
                    // the batch had not yet dispositioned to the retry queue
                    // (which dead-letters back to the lane queue after the TTL)
                    // and ACK it. Duplicates are deduped by durable admission
                    // when the control plane recovers.
                    _logger.LogError(
                        exception,
                        "RabbitMQ KubeJob batch admission failed for {Count} deliveries; republishing them to the retry queue",
                        batch.Count);
                    foreach (var delivery in batch)
                    {
                        if (dispositioned.Contains(delivery.DeliveryTag))
                        {
                            continue;
                        }

                        try
                        {
                            var envelope = JsonSerializer.Deserialize<ExecutionEnvelope>(
                                delivery.Body.Span,
                                SerializerOptions);
                            if (envelope is null)
                            {
                                Reject(channel, channelGate, delivery.DeliveryTag,
                                    "admission_failed_and_body_unparseable");
                                continue;
                            }

                            RepublishForRetry(
                                delivery,
                                envelope,
                                channel,
                                channelGate,
                                publishChannel,
                                publishChannelGate);
                        }
                        catch (Exception retryException)
                        {
                            Reject(channel, channelGate, delivery.DeliveryTag,
                                $"admission_failure_republish_failed: {retryException.Message}");
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task ProcessBatchAsync(
        IReadOnlyList<BasicDeliverEventArgs> batch,
        IModel channel,
        object channelGate,
        IModel publishChannel,
        object publishChannelGate,
        ISet<ulong> dispositioned,
        CancellationToken cancellationToken)
    {
        // Parse every delivery first so malformed envelopes are rejected
        // immediately and never enter the admission transaction.
        var envelopes = new ExecutionEnvelope?[batch.Count];
        var validIndexes = new List<int>(batch.Count);
        for (var index = 0; index < batch.Count; index++)
        {
            try
            {
                envelopes[index] = JsonSerializer.Deserialize<ExecutionEnvelope>(
                    batch[index].Body.Span,
                    SerializerOptions)
                    ?? throw new JsonException("RabbitMQ execution envelope was empty.");
                validIndexes.Add(index);
            }
            catch (JsonException exception)
            {
                Reject(channel, channelGate, batch[index].DeliveryTag, exception.Message);
                dispositioned.Add(batch[index].DeliveryTag);
                _logger.LogWarning(
                    exception,
                    "Rejected malformed RabbitMQ execution envelope {DeliveryTag}",
                    batch[index].DeliveryTag);
            }
        }

        if (validIndexes.Count == 0)
        {
            return;
        }

        var validEnvelopes = validIndexes.Select(index => envelopes[index]!).ToArray();
        var outcomes = await _runtime.AdmitEnvelopesAsync(validEnvelopes, cancellationToken);
        if (outcomes.Count != validIndexes.Count)
        {
            throw new InvalidOperationException(
                $"Batch admission returned {outcomes.Count} results for {validIndexes.Count} envelopes.");
        }

        for (var outcomeIndex = 0; outcomeIndex < outcomes.Count; outcomeIndex++)
        {
            var delivery = batch[validIndexes[outcomeIndex]];
            var envelope = validEnvelopes[outcomeIndex];
            var outcome = outcomes[outcomeIndex];

            if (outcome.Completion is not null)
            {
                // Admitted: the execution runs asynchronously; ACK it when the
                // attempt completes durably. Never block the batch loop on a
                // slow handler, so the next batch's admission is not serialized
                // behind execution.
                dispositioned.Add(delivery.DeliveryTag);
                _ = CompleteEnvelopeAsync(
                    delivery,
                    envelope,
                    outcome.Completion,
                    channel,
                    channelGate,
                    publishChannel,
                    publishChannelGate);
                continue;
            }

            switch (outcome.Status)
            {
                case ExecutionEnvelopeProcessingStatus.Completed:
                    Ack(channel, channelGate, delivery.DeliveryTag);
                    dispositioned.Add(delivery.DeliveryTag);
                    _logger.LogDebug(
                        "ACKed RabbitMQ execution envelope {EventId} for Run {RunId}",
                        envelope.EventId,
                        envelope.RunId);
                    break;
                case ExecutionEnvelopeProcessingStatus.Reject:
                    Reject(channel, channelGate, delivery.DeliveryTag, outcome.Reason);
                    dispositioned.Add(delivery.DeliveryTag);
                    break;
                case ExecutionEnvelopeProcessingStatus.Retry:
                    await RepublishOrReconcileAsync(
                        delivery,
                        envelope,
                        "worker_retry",
                        channel,
                        channelGate,
                        publishChannel,
                        publishChannelGate,
                        cancellationToken);
                    dispositioned.Add(delivery.DeliveryTag);
                    break;
                default:
                    Reject(channel, channelGate, delivery.DeliveryTag, "invalid_admission_outcome");
                    dispositioned.Add(delivery.DeliveryTag);
                    break;
            }
        }
    }

    /// <summary>
    /// Waits for an admitted attempt to complete durably, then ACKs/Rejects/
    /// republishes the delivery accordingly. Fire-and-forget: failures only
    /// leave the delivery unacked, which the broker requeues.
    /// </summary>
    private async Task CompleteEnvelopeAsync(
        BasicDeliverEventArgs delivery,
        ExecutionEnvelope envelope,
        Task<ExecutionEnvelopeProcessingResult> completion,
        IModel channel,
        object channelGate,
        IModel publishChannel,
        object publishChannelGate)
    {
        try
        {
            var result = await completion;
            switch (result.Status)
            {
                case ExecutionEnvelopeProcessingStatus.Completed:
                    Ack(channel, channelGate, delivery.DeliveryTag);
                    _logger.LogDebug(
                        "ACKed RabbitMQ execution envelope {EventId} for Run {RunId}",
                        envelope.EventId,
                        envelope.RunId);
                    break;
                case ExecutionEnvelopeProcessingStatus.Reject:
                    Reject(channel, channelGate, delivery.DeliveryTag, result.Reason);
                    break;
                case ExecutionEnvelopeProcessingStatus.Retry:
                    await RepublishOrReconcileAsync(
                        delivery,
                        envelope,
                        "worker_retry",
                        channel,
                        channelGate,
                        publishChannel,
                        publishChannelGate,
                        CancellationToken.None);
                    break;
            }
        }
        catch (Exception exception)
        {
            // The attempt could not be resolved durably (e.g. the session was
            // torn down mid-execution). Leave the delivery unacked; the broker
            // requeues it for another worker.
            _logger.LogWarning(
                exception,
                "RabbitMQ execution envelope {EventId} for Run {RunId} was left unacked after an in-flight attempt; broker will redeliver",
                envelope.EventId,
                envelope.RunId);
        }
    }

    private Task ProcessCancelDeliveryAsync(
        IModel channel,
        object channelGate,
        BasicDeliverEventArgs delivery)
    {
        try
        {
            var cancel = JsonSerializer.Deserialize<CancelEnvelope>(
                delivery.Body.Span,
                SerializerOptions)
                ?? throw new JsonException("RabbitMQ cancel envelope was empty.");
            if (string.IsNullOrWhiteSpace(cancel.RunId))
            {
                throw new JsonException("RabbitMQ cancel envelope did not contain a runId.");
            }

            _runtime.TryCancelRun(cancel.RunId);
            Ack(channel, channelGate, delivery.DeliveryTag);
            _logger.LogDebug(
                "ACKed RabbitMQ cancel signal for Run {RunId}",
                cancel.RunId);
        }
        catch (JsonException exception)
        {
            Reject(channel, channelGate, delivery.DeliveryTag, exception.Message);
            _logger.LogWarning(
                exception,
                "Rejected malformed RabbitMQ cancel envelope {DeliveryTag}",
                delivery.DeliveryTag);
        }
        catch (Exception exception)
        {
            Nack(channel, channelGate, delivery.DeliveryTag, exception.Message);
            _logger.LogError(
                exception,
                "Transient failure processing RabbitMQ cancel envelope {DeliveryTag}; requeued",
                delivery.DeliveryTag);
        }

        return Task.CompletedTask;
    }

    private async Task RepublishOrReconcileAsync(
        BasicDeliverEventArgs delivery,
        ExecutionEnvelope envelope,
        string reason,
        IModel channel,
        object channelGate,
        IModel publishChannel,
        object publishChannelGate,
        CancellationToken cancellationToken)
    {
        if (GetBrokerRetryCount(delivery.BasicProperties) >= _options.MaxBrokerRetryAttempts)
        {
            try
            {
                var scheduled = await _runtimeClient.RequeueExecutionAsync(
                    new RequeueExecutionRequest(
                        envelope.RunId,
                        DateTimeOffset.UtcNow + _options.BrokerRetryReconciliationDelay),
                    cancellationToken);
                Ack(channel, channelGate, delivery.DeliveryTag);
                _metrics?.ReconciliationHandedOff();
                _logger.LogWarning(
                    "ACKed RabbitMQ execution envelope {EventId} after broker retry budget; durable reconciliation scheduled={Scheduled} for Run {RunId}",
                    envelope.EventId,
                    scheduled,
                    envelope.RunId);
            }
            catch (Exception reconciliationException)
            {
                // Reconciliation failed too: drop into the DLQ via requeue=false
                // rather than NACK-ing back to the head of the queue, which
                // would loop indefinitely.
                Reject(channel, channelGate, delivery.DeliveryTag,
                    $"{reason}; durable reconciliation failed: {reconciliationException.Message}");
            }

            return;
        }

        try
        {
            RepublishForRetry(
                delivery,
                envelope,
                channel,
                channelGate,
                publishChannel,
                publishChannelGate);
            _metrics?.BrokerRetried();
        }
        catch (Exception retryException)
        {
            // Same NACK-loop risk: prefer reject (no requeue) so the broker
            // routes the envelope through its DLX. The durable outbox still
            // owns correctness, so we accept a DLQ entry as a poison-pill
            // signal rather than spinning forever.
            Reject(channel, channelGate, delivery.DeliveryTag,
                $"{reason}; retry publication failed: {retryException.Message}");
        }
    }

    /// <summary>
    /// Republishes a delivery to the group TTL retry queue on the dedicated
    /// publish channel, then ACKs the original delivery on the consumer
    /// channel. Publisher-confirm waits block only the publish channel, never
    /// the channel carrying deliveries and ACKs. The publish channel is shared
    /// between the batch loop and the fire-and-forget completion path, so the
    /// whole subscribe-publish-confirm-unsubscribe sequence is serialized
    /// through <paramref name="publishChannelGate"/>: RabbitMQ.Client's IModel
    /// is not thread-safe and WaitForConfirms aggregates per-channel state.
    /// </summary>
    private void RepublishForRetry(
        BasicDeliverEventArgs delivery,
        ExecutionEnvelope envelope,
        IModel channel,
        object channelGate,
        IModel publishChannel,
        object publishChannelGate)
    {
        var messageId = delivery.BasicProperties.MessageId;
        BasicReturnEventArgs? returned = null;
        EventHandler<BasicReturnEventArgs> returnHandler = (_, args) =>
        {
            if (string.Equals(args.BasicProperties.MessageId, messageId, StringComparison.Ordinal))
            {
                returned = args;
            }
        };
        lock (publishChannelGate)
        {
        publishChannel.BasicReturn += returnHandler;
        try
        {
            var retryCount = GetBrokerRetryCount(delivery.BasicProperties) + 1;
            var properties = delivery.BasicProperties;
            var headers = properties.Headers is null
                ? new Dictionary<string, object>()
                : new Dictionary<string, object>(properties.Headers);
            headers[BrokerRetryCountHeader] = retryCount;
            properties.Headers = headers;

            // Re-derive the lane from the envelope's PartitionKey so the
            // retry lands on the same lane's retry queue. The retry queue
            // dead-letters without a routing-key override, preserving this
            // lane-suffixed key and routing the retried message back to the
            // same lane dispatch queue. N=1 collapses to envelope.Queue.
            var lane = ExecutionLaneRouter.GetLane(envelope.PartitionKey, _options.ExecutionLaneCount);
            var routingKey = _options.GetLaneRoutingKey(envelope.Queue, lane);
            publishChannel.BasicPublish(
                exchange: _options.GetRetryExchangeName(),
                routingKey: routingKey,
                mandatory: true,
                basicProperties: delivery.BasicProperties,
                body: delivery.Body.ToArray());
            if (!publishChannel.WaitForConfirms(_options.PublisherConfirmTimeout))
            {
                throw new IOException(
                    $"RabbitMQ did not confirm retry publication for delivery {delivery.DeliveryTag}.");
            }

            if (returned is not null)
            {
                throw new IOException(
                    $"RabbitMQ could not route retry delivery {delivery.DeliveryTag} for queue '{routingKey}'.");
            }

            Ack(channel, channelGate, delivery.DeliveryTag);
        }
        finally
        {
            publishChannel.BasicReturn -= returnHandler;
        }
        }
    }

    private const string BrokerRetryCountHeader = "x-kubejob-broker-retry-count";

    private static int GetBrokerRetryCount(IBasicProperties properties)
    {
        if (properties.Headers is null
            || !properties.Headers.TryGetValue(BrokerRetryCountHeader, out var value))
        {
            return 0;
        }

        // The header is written only by our own republish path as an int; any
        // other encoding is a foreign producer and is treated as zero.
        return value is int integer ? integer : 0;
    }

    private sealed record CancelEnvelope(string RunId);

    private void Ack(IModel channel, object gate, ulong deliveryTag)
    {
        lock (gate)
        {
            channel.BasicAck(deliveryTag, multiple: false);
        }
    }

    private void Reject(IModel channel, object gate, ulong deliveryTag, string? reason)
    {
        lock (gate)
        {
            channel.BasicReject(deliveryTag, requeue: false);
        }

        _logger.LogWarning(
            "Rejected RabbitMQ execution envelope {DeliveryTag}: {Reason}",
            deliveryTag,
            reason ?? "unspecified");
    }

    private void Nack(IModel channel, object gate, ulong deliveryTag, string? reason)
    {
        lock (gate)
        {
            channel.BasicNack(deliveryTag, multiple: false, requeue: true);
        }

        _logger.LogDebug(
            "Requeued RabbitMQ execution envelope {DeliveryTag}: {Reason}",
            deliveryTag,
            reason ?? "unspecified");
    }
}
