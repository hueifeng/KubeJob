using System.Security.Cryptography;
using System.Text.Json;
using KubeJob.Core.Domain;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Options;
using KubeJob.Server.Data;
using KubeJob.Server.Options;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Services;

public sealed class KubeJobClient : IKubeJobClient
{
    private const int HardPayloadLimit = 16 * 1024 * 1024;
    private readonly IKubeJobSubmissionRepository _repository;
    private readonly KubeJobClientOptions _options;

    public KubeJobClient(IKubeJobSubmissionRepository repository, IOptions<KubeJobClientOptions> options)
    {
        _repository = repository;
        _options = options.Value;
        if (_options.MaxPayloadBytes is <= 0 or > HardPayloadLimit)
            throw new InvalidOperationException($"MaxPayloadBytes must be between 1 and {HardPayloadLimit}.");
    }

    public async Task<JobSubmissionResult> EnqueueAsync<TPayload>(string jobName, TPayload payload,
        JobEnqueueOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobName);
        options ??= new JobEnqueueOptions();
        if (options.PayloadSchemaVersion <= 0) throw new ArgumentOutOfRangeException(nameof(options.PayloadSchemaVersion));
        if (options.IdempotencyKey.Length > 200 || options.QueueName.Length > 100)
            throw new ArgumentException("Idempotency key or queue name is too long.");

        var payloadUtf8 = JsonSerializer.SerializeToUtf8Bytes(payload, _options.PayloadJsonOptions);
        if (payloadUtf8.Length > _options.MaxPayloadBytes)
            throw new InvalidOperationException($"Payload is {payloadUtf8.Length} bytes; limit is {_options.MaxPayloadBytes}.");

        return await _repository.SubmitAsync(new JobSubmissionCommand
        {
            JobName = jobName,
            PayloadUtf8 = payloadUtf8,
            PayloadHash = SHA256.HashData(payloadUtf8),
            PayloadSchemaVersion = options.PayloadSchemaVersion,
            QueueName = options.QueueName,
            Priority = options.Priority,
            IdempotencyKey = options.IdempotencyKey,
            AvailableAt = options.AvailableAt
        }, cancellationToken);
    }

    public Task<bool> CancelRunAsync(string runId, string reason = "Canceled by user",
        CancellationToken cancellationToken = default) =>
        _repository.CancelRunAsync(runId, TrimReason(reason), cancellationToken);

    public Task<int> CancelBatchAsync(string batchId, string reason = "Canceled by user",
        CancellationToken cancellationToken = default) =>
        _repository.CancelBatchAsync(batchId, TrimReason(reason), cancellationToken);

    private static string TrimReason(string value) => value.Length <= 2000 ? value : value[..2000];
}
