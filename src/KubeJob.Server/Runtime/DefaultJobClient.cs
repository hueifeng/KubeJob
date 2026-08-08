using System.Diagnostics;
using System.Text.Json;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Core.Transport;
using KubeJob.Server.ControlPlane;

namespace KubeJob.Server.Runtime;

/// <summary>
/// Unified job client. Queue configuration selects exactly one execution
/// authority: PostgresManaged submits to the control plane, while BrokerNative
/// publishes a self-contained executable message directly through the selected
/// transport adapter.
/// </summary>
public sealed class DefaultJobClient : IJobClient
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly JobControlPlane _controlPlane;
    private readonly IQueueRuntimeResolver _runtimeResolver;
    private readonly IMessageTransportRegistry _transportRegistry;

    public DefaultJobClient(
        JobControlPlane controlPlane,
        IQueueRuntimeResolver runtimeResolver,
        IMessageTransportRegistry transportRegistry)
    {
        _controlPlane = controlPlane;
        _runtimeResolver = runtimeResolver;
        _transportRegistry = transportRegistry;
    }

    public ValueTask<JobHandle> EnqueueAsync<TPayload>(
        JobKey<TPayload> job,
        TPayload payload,
        CancellationToken cancellationToken = default)
    {
        return EnqueueAsync(job, payload, new JobEnqueueOptions(), cancellationToken);
    }

    public async ValueTask<JobHandle> EnqueueAsync<TPayload>(
        JobKey<TPayload> job,
        TPayload payload,
        JobEnqueueOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        EnsureJobKey(job);

        var queue = options.ResolveQueue(job.Value);
        var route = _runtimeResolver.Resolve(queue);

        return route.Mode switch
        {
            QueueRuntimeMode.PostgresManaged => await EnqueueManagedAsync(
                job.Value,
                payload,
                queue,
                options,
                cancellationToken),

            QueueRuntimeMode.BrokerNative => await EnqueueBrokerNativeAsync(
                job.Value,
                payload,
                queue,
                route,
                options,
                cancellationToken),

            _ => throw new InvalidOperationException(
                $"Unsupported Queue runtime mode '{route.Mode}' for queue '{queue}'.")
        };
    }

    public async ValueTask<IReadOnlyList<JobHandle>> EnqueueBatchAsync<TPayload>(
        JobKey<TPayload> job,
        IReadOnlyList<(TPayload Payload, JobEnqueueOptions? Options)> batch,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0)
        {
            return Array.Empty<JobHandle>();
        }

        // The public batch API needs one bounded allocation/publish envelope
        // regardless of authority. For PostgresManaged this protects the
        // database transaction; for BrokerNative it also bounds serialization,
        // publisher-lock hold time, and one confirm window.
        _controlPlane.ValidateSubmissionBatchSize(batch.Count);
        EnsureJobKey(job);

        var prepared = new PreparedSubmission<TPayload>[batch.Count];
        QueueRuntimeMode? commonMode = null;
        for (var i = 0; i < batch.Count; i++)
        {
            var (payload, suppliedOptions) = batch[i];
            var options = suppliedOptions ?? new JobEnqueueOptions();
            var queue = options.ResolveQueue(job.Value);
            var route = _runtimeResolver.Resolve(queue);
            prepared[i] = new PreparedSubmission<TPayload>(payload, options, queue, route);

            commonMode ??= route.Mode;
            if (commonMode != route.Mode)
            {
                throw new InvalidOperationException(
                    "One EnqueueBatchAsync call cannot mix PostgresManaged and BrokerNative queues. " +
                    "Split the batch by Queue runtime so failure/atomicity semantics stay explicit.");
            }
        }

        if (commonMode == QueueRuntimeMode.PostgresManaged)
        {
            var requests = new EnqueueJobRequest[prepared.Length];
            for (var i = 0; i < prepared.Length; i++)
            {
                var item = prepared[i];
                var payloadJson = JsonSerializer.Serialize(item.Payload, SerializerOptions);
                requests[i] = CreateManagedRequest(
                    job.Value,
                    payloadJson,
                    item.Queue,
                    item.Options);
            }

            var receipts = await _controlPlane.SubmitBatchAsync(requests, cancellationToken);
            var handles = new JobHandle[receipts.Count];
            for (var i = 0; i < receipts.Count; i++)
            {
                handles[i] = receipts[i].Handle;
            }

            return handles;
        }

        // BrokerNative cannot provide the database transaction atomicity of
        // PostgresManaged. A transport may optimize this into one publisher-
        // confirm batch, but a failure can still be observed after the broker
        // accepted a subset or all messages. Retrying is therefore at-least-
        // once. IdempotencyKey is carried as metadata only; KubeJob does not
        // currently deduplicate BrokerNative deliveries on that value.
        var brokerSubmissions = new BrokerNativeSubmission[prepared.Length];
        for (var i = 0; i < prepared.Length; i++)
        {
            var item = prepared[i];
            brokerSubmissions[i] = CreateBrokerNativeSubmission(
                job.Value,
                item.Payload,
                item.Queue,
                item.Route,
                item.Options);
        }

        foreach (var group in brokerSubmissions.GroupBy(
                     submission => submission.TransportId,
                     StringComparer.OrdinalIgnoreCase))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var publisher = _transportRegistry.GetRequiredPublisher(group.Key);
            var requests = group.Select(submission => submission.Request).ToArray();
            if (publisher is IMessageTransportBatchPublisher batchPublisher)
            {
                await batchPublisher.PublishBatchAsync(requests, cancellationToken);
            }
            else
            {
                foreach (var request in requests)
                {
                    await publisher.PublishAsync(request, cancellationToken);
                }
            }
        }

        return brokerSubmissions.Select(submission => submission.Handle).ToArray();
    }

    public async ValueTask<JobStatusSnapshot?> GetStatusAsync(
        string jobId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);

        // V3 currently has no BrokerNative history projection. This remains a
        // PostgresManaged status query, so a BrokerNative MessageId normally
        // returns null instead of fabricating a strong lifecycle state.
        return await _controlPlane.GetStatusAsync(jobId, cancellationToken);
    }

    public ValueTask<bool> CancelAsync(
        string jobId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);

        // Strong cancellation is a PostgresManaged capability. There is no
        // BrokerNative queued-cancel protocol in V3 today; passing a BrokerNative
        // MessageId therefore cannot cancel a broker delivery through this API.
        return _controlPlane.RequestCancelAsync(jobId, reason, cancellationToken);
    }

    private async ValueTask<JobHandle> EnqueueManagedAsync<TPayload>(
        string jobKey,
        TPayload payload,
        string queue,
        JobEnqueueOptions options,
        CancellationToken cancellationToken)
    {
        var payloadJson = JsonSerializer.Serialize(payload, SerializerOptions);
        var receipt = await _controlPlane.SubmitAsync(
            CreateManagedRequest(jobKey, payloadJson, queue, options),
            cancellationToken);
        return receipt.Handle;
    }

    private async ValueTask<JobHandle> EnqueueBrokerNativeAsync<TPayload>(
        string jobKey,
        TPayload payload,
        string queue,
        QueueRuntimeRoute route,
        JobEnqueueOptions options,
        CancellationToken cancellationToken)
    {
        var submission = CreateBrokerNativeSubmission(
            jobKey,
            payload,
            queue,
            route,
            options);
        var publisher = _transportRegistry.GetRequiredPublisher(submission.TransportId);
        await publisher.PublishAsync(submission.Request, cancellationToken);
        return submission.Handle;
    }

    private static BrokerNativeSubmission CreateBrokerNativeSubmission<TPayload>(
        string jobKey,
        TPayload payload,
        string queue,
        QueueRuntimeRoute route,
        JobEnqueueOptions options)
    {
        ValidateBrokerNativeOptions(options, queue);
        var transportId = route.TransportId
            ?? throw new InvalidOperationException(
                $"BrokerNative queue '{queue}' does not have a transport configured.");

        var payloadJson = JsonSerializer.Serialize(payload, SerializerOptions);
        var messageId = Guid.NewGuid().ToString("N");
        var activity = Activity.Current;
        var message = new BrokerNativeJobMessage
        {
            MessageId = messageId,
            JobKey = jobKey,
            Queue = queue,
            PayloadJson = payloadJson,
            EnqueuedAt = DateTimeOffset.UtcNow,
            Attempt = 1,
            MaxAttempts = options.MaxAttempts,
            TimeoutSeconds = checked((int)Math.Ceiling(options.Timeout.TotalSeconds)),
            IdempotencyKey = options.IdempotencyKey,
            CorrelationId = activity?.TraceId.ToString(),
            TraceParent = activity?.Id
        };
        message.Validate();

        var body = JsonSerializer.SerializeToUtf8Bytes(message, SerializerOptions);
        var request = new TransportPublishRequest(
            TransportMessageKind.Job,
            queue,
            new TransportMessage(
                messageId,
                "kubejob.broker-native.job.v1",
                body,
                message.Headers,
                message.CorrelationId,
                message.PartitionKey),
            RoutingKey: queue);
        return new BrokerNativeSubmission(
            transportId,
            request,
            new JobHandle(messageId));
    }

    private static EnqueueJobRequest CreateManagedRequest(
        string jobKey,
        string payloadJson,
        string queue,
        JobEnqueueOptions options)
    {
        var timeoutSeconds = checked((int)Math.Ceiling(options.Timeout.TotalSeconds));
        return new EnqueueJobRequest(
            jobKey,
            payloadJson,
            queue,
            options.Priority,
            options.NotBefore?.ToUniversalTime(),
            options.IdempotencyKey,
            options.ConcurrencyKey,
            options.MaxAttempts,
            timeoutSeconds,
            RetryPolicy: options.RetryPolicy,
            Continuation: options.Continuation,
            Compensation: options.Compensation);
    }

    private static void ValidateBrokerNativeOptions(
        JobEnqueueOptions options,
        string queue)
    {
        options.Validate();

        if (options.Priority != 0)
        {
            throw Unsupported(queue, nameof(options.Priority));
        }

        if (options.NotBefore is not null)
        {
            throw Unsupported(queue, nameof(options.NotBefore));
        }

        if (!string.IsNullOrWhiteSpace(options.ConcurrencyKey))
        {
            throw new NotSupportedException(
                $"BrokerNative queue '{queue}' does not use managed ConcurrencyKey. " +
                "Use a transport-native partition/ordering policy when that capability is implemented for the Queue.");
        }

        if (options.RetryPolicy is not null)
        {
            throw Unsupported(queue, nameof(options.RetryPolicy));
        }

        if (options.Continuation is not null)
        {
            throw Unsupported(queue, nameof(options.Continuation));
        }

        if (options.Compensation is not null)
        {
            throw Unsupported(queue, nameof(options.Compensation));
        }
    }

    private static NotSupportedException Unsupported(string queue, string option)
        => new(
            $"Job option '{option}' is not supported by BrokerNative queue '{queue}' yet. " +
            "KubeJob rejects it instead of silently changing its semantics.");

    private static void EnsureJobKey<TPayload>(JobKey<TPayload> job)
    {
        if (job.IsEmpty)
        {
            throw new ArgumentException("The job key must be initialized.", nameof(job));
        }
    }

    private sealed record PreparedSubmission<TPayload>(
        TPayload Payload,
        JobEnqueueOptions Options,
        string Queue,
        QueueRuntimeRoute Route);

    private sealed record BrokerNativeSubmission(
        string TransportId,
        TransportPublishRequest Request,
        JobHandle Handle);
}
