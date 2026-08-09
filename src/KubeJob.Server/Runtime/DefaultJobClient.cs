using System.Diagnostics;
using System.Text;
using System.Text.Json;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Core.Transport;
using KubeJob.Server.ControlPlane;
using Microsoft.Extensions.Options;

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
    private readonly JobRuntimeOptions _runtimeOptions;

    public DefaultJobClient(
        JobControlPlane controlPlane,
        IQueueRuntimeResolver runtimeResolver,
        IMessageTransportRegistry transportRegistry,
        IOptions<JobRuntimeOptions> runtimeOptions)
    {
        _controlPlane = controlPlane;
        _runtimeResolver = runtimeResolver;
        _transportRegistry = transportRegistry;
        _runtimeOptions = runtimeOptions.Value;
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

        EnsureJobKey(job);

        var prepared = new PreparedSubmission<TPayload>[batch.Count];
        QueueRuntimeMode? commonMode = null;
        for (var i = 0; i < batch.Count; i++)
        {
            var (payload, suppliedOptions) = batch[i];
            var options = suppliedOptions ?? new JobEnqueueOptions();
            var queue = options.ResolveQueue(job.Value);
            var route = _runtimeResolver.Resolve(queue);
            if (route.Mode is not (QueueRuntimeMode.PostgresManaged or QueueRuntimeMode.BrokerNative))
            {
                throw new InvalidOperationException(
                    $"Unsupported Queue runtime mode '{route.Mode}' for queue '{queue}'.");
            }

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
            _controlPlane.ValidateSubmissionBatchSize(batch.Count);
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

        // A broker publish batch cannot provide the database transaction
        // atomicity of PostgresManaged. Each message is independently confirmed
        // by its transport; callers must use a business-idempotent handler and
        // account for partial success when retrying a failed batch.
        var brokerHandles = new JobHandle[prepared.Length];
        for (var i = 0; i < prepared.Length; i++)
        {
            var item = prepared[i];
            brokerHandles[i] = await EnqueueBrokerNativeAsync(
                job.Value,
                item.Payload,
                item.Queue,
                item.Route,
                item.Options,
                cancellationToken);
        }

        return brokerHandles;
    }

    /// <summary>
    /// Routes a transport-neutral HTTP submission through the same authority
    /// decision used by the typed in-process client.
    /// </summary>
    public async ValueTask<JobSubmissionReceipt> SubmitAsync(
        EnqueueJobRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var queue = ResolveQueue(request.Queue);
        var route = _runtimeResolver.Resolve(queue);
        if (route.Mode == QueueRuntimeMode.PostgresManaged)
        {
            return await _controlPlane.SubmitAsync(request with { Queue = queue }, cancellationToken);
        }

        if (route.Mode != QueueRuntimeMode.BrokerNative)
        {
            throw new ControlPlaneValidationException(
                "unsupported_queue_runtime",
                $"Queue '{queue}' has unsupported runtime mode '{route.Mode}'.");
        }

        var options = CreateBrokerNativeOptions(request, queue);
        var handle = await EnqueueBrokerNativePayloadAsync(
            request.JobKey,
            request.PayloadJson,
            queue,
            route,
            options,
            cancellationToken);
        return new JobSubmissionReceipt(handle, Existing: false);
    }

    /// <summary>
    /// Routes a raw batch through one runtime authority. Managed batches are
    /// transactional; BrokerNative batches are confirmed independently.
    /// </summary>
    public async ValueTask<IReadOnlyList<JobSubmissionReceipt>> SubmitBatchAsync(
        IReadOnlyList<EnqueueJobRequest> requests,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(requests);
        _controlPlane.ValidateSubmissionBatchSize(requests.Count);
        if (requests.Count == 0)
        {
            return Array.Empty<JobSubmissionReceipt>();
        }

        QueueRuntimeMode? mode = null;
        var prepared = new RawSubmission[requests.Count];
        for (var index = 0; index < requests.Count; index++)
        {
            var request = requests[index] ?? throw new ControlPlaneValidationException(
                "invalid_job_submission",
                $"Submission batch item at index {index} cannot be null.");
            var queue = ResolveQueue(request.Queue);
            var route = _runtimeResolver.Resolve(queue);
            if (route.Mode is not (QueueRuntimeMode.PostgresManaged or QueueRuntimeMode.BrokerNative))
            {
                throw new ControlPlaneValidationException(
                    "unsupported_queue_runtime",
                    $"Queue '{queue}' has unsupported runtime mode '{route.Mode}'.");
            }

            mode ??= route.Mode;
            if (mode != route.Mode)
            {
                throw new ControlPlaneValidationException(
                    "mixed_queue_runtime_batch",
                    "One submission batch cannot mix PostgresManaged and BrokerNative queues.");
            }

            prepared[index] = new RawSubmission(request with { Queue = queue }, route);
        }

        if (mode == QueueRuntimeMode.PostgresManaged)
        {
            return await _controlPlane.SubmitBatchAsync(
                prepared.Select(item => item.Request).ToArray(),
                cancellationToken);
        }

        var receipts = new JobSubmissionReceipt[prepared.Length];
        for (var index = 0; index < prepared.Length; index++)
        {
            var item = prepared[index];
            var options = CreateBrokerNativeOptions(item.Request, item.Request.Queue);
            var handle = await EnqueueBrokerNativePayloadAsync(
                item.Request.JobKey,
                item.Request.PayloadJson,
                item.Request.Queue,
                item.Route,
                options,
                cancellationToken);
            receipts[index] = new JobSubmissionReceipt(handle, Existing: false);
        }

        return receipts;
    }

    public async ValueTask<JobStatusSnapshot?> GetStatusAsync(
        string jobId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);

        // BrokerNative execution history is an asynchronous projection. Until a
        // projection contains this MessageId, the Managed query naturally
        // returns null rather than fabricating a strong state.
        return await _controlPlane.GetStatusAsync(jobId, cancellationToken);
    }

    public ValueTask<bool> CancelAsync(
        string jobId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);

        // Strong cancellation is a PostgresManaged capability. BrokerNative
        // queued cancellation is intentionally best-effort and will be exposed
        // separately rather than pretending this Managed operation is strong.
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
        var payloadJson = JsonSerializer.Serialize(payload, SerializerOptions);
        return await EnqueueBrokerNativePayloadAsync(
            jobKey,
            payloadJson,
            queue,
            route,
            options,
            cancellationToken);
    }

    private async ValueTask<JobHandle> EnqueueBrokerNativePayloadAsync(
        string jobKey,
        string payloadJson,
        string queue,
        QueueRuntimeRoute route,
        JobEnqueueOptions options,
        CancellationToken cancellationToken)
    {
        ValidateBrokerNativeRequest(jobKey, payloadJson, options, queue);
        var transportId = route.TransportId
            ?? throw new InvalidOperationException(
                $"BrokerNative queue '{queue}' does not have a transport configured.");
        var publisher = _transportRegistry.GetRequiredPublisher(transportId);
        ValidateBrokerNativeCapabilities(publisher, queue, options.MaxAttempts);

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
            RetryPolicy = options.RetryPolicy,
            IdempotencyKey = options.IdempotencyKey,
            CorrelationId = activity?.TraceId.ToString(),
            TraceParent = activity?.Id
        };
        message.Validate();

        var body = JsonSerializer.SerializeToUtf8Bytes(message, SerializerOptions);
        await publisher.PublishAsync(
            new TransportPublishRequest(
                TransportMessageKind.Job,
                queue,
                new TransportMessage(
                    messageId,
                    "kubejob.broker-native.job.v1",
                    body,
                    message.Headers,
                    message.CorrelationId,
                    message.PartitionKey),
                RoutingKey: queue),
            cancellationToken);

        return new JobHandle(messageId);
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
            RetryPolicy: options.RetryPolicy);
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

        // BrokerNative intentionally has no durable KubeJob-side deduplication
        // store. Keeping this option accepted would imply the PostgresManaged
        // idempotency contract while retrying a timed-out publish could execute
        // the handler more than once. Applications needing that guarantee must
        // use a PostgresManaged queue or make the handler idempotent itself.
        if (!string.IsNullOrWhiteSpace(options.IdempotencyKey))
        {
            throw Unsupported(queue, nameof(options.IdempotencyKey));
        }

        if (!string.IsNullOrWhiteSpace(options.ConcurrencyKey))
        {
            throw new NotSupportedException(
                $"BrokerNative queue '{queue}' does not use managed ConcurrencyKey. " +
                "Use PartitionKey/partitioned routing when ordering is enabled for that Queue.");
        }

        if (options.RetryPolicy is not null)
        {
            throw Unsupported(queue, nameof(options.RetryPolicy));
        }

    }

    private void ValidateBrokerNativeRequest(
        string jobKey,
        string payloadJson,
        JobEnqueueOptions options,
        string queue)
    {
        try
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(jobKey);
            ArgumentException.ThrowIfNullOrWhiteSpace(payloadJson);
            if (jobKey.Length > 300)
            {
                throw new ArgumentException("JobKey cannot exceed 300 characters.", nameof(jobKey));
            }

            if (Encoding.UTF8.GetByteCount(payloadJson) > _runtimeOptions.MaxPayloadBytes)
            {
                throw new ArgumentException(
                    $"Payload exceeds the configured maximum of {_runtimeOptions.MaxPayloadBytes} bytes.",
                    nameof(payloadJson));
            }

            using var _ = JsonDocument.Parse(payloadJson);
            ValidateBrokerNativeOptions(options, queue);
        }
        catch (ArgumentException exception)
        {
            throw new ControlPlaneValidationException("invalid_job_submission", exception.Message);
        }
        catch (JsonException exception)
        {
            throw new ControlPlaneValidationException("invalid_job_submission", exception.Message);
        }
    }

    private static JobEnqueueOptions CreateBrokerNativeOptions(
        EnqueueJobRequest request,
        string queue)
    {
        try
        {
            return new JobEnqueueOptions
            {
                Queue = queue,
                Priority = request.Priority,
                NotBefore = request.NotBefore,
                IdempotencyKey = request.IdempotencyKey,
                ConcurrencyKey = request.ConcurrencyKey,
                MaxAttempts = request.MaxAttempts,
                Timeout = TimeSpan.FromSeconds(request.TimeoutSeconds),
                RetryPolicy = request.RetryPolicy
            };
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new ControlPlaneValidationException("invalid_job_submission", exception.Message);
        }
    }

    private static string ResolveQueue(string queue)
    {
        try
        {
            return Core.Queues.LogicalQueueName.Normalize(queue, nameof(queue));
        }
        catch (ArgumentException exception)
        {
            throw new ControlPlaneValidationException("invalid_job_submission", exception.Message);
        }
    }

    private static void ValidateBrokerNativeCapabilities(
        IMessageTransportPublisher publisher,
        string queue,
        int maxAttempts)
    {
        if (!publisher.Capabilities.HasFlag(MessageTransportCapabilities.DurablePublish))
        {
            throw new NotSupportedException(
                $"BrokerNative queue '{queue}' requires durable publish, but transport " +
                $"'{publisher.TransportId}' does not advertise {nameof(MessageTransportCapabilities.DurablePublish)}.");
        }

        if (maxAttempts > 1
            && !publisher.Capabilities.HasFlag(MessageTransportCapabilities.DeadLetter))
        {
            throw new NotSupportedException(
                $"BrokerNative queue '{queue}' requests retries, but transport " +
                $"'{publisher.TransportId}' does not advertise {nameof(MessageTransportCapabilities.DeadLetter)}.");
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

    private sealed record RawSubmission(
        EnqueueJobRequest Request,
        QueueRuntimeRoute Route);
}
