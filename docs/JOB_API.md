# Job API V2

## Typed Job

```csharp
[KubeJob("rebuild-index", Cron = "0 */5 * * *", TotalShards = 16,
    ExecuteModel = ExecuteModel.Sharding, TimeoutSeconds = 600, MaxRetries = 3)]
[KubeJobPayload(schemaVersion: 2, HandlerVersion = "2026.07")]
public sealed class RebuildIndexJob : IKubeJob<RebuildIndexPayload>
{
    public async ValueTask ExecuteAsync(
        RebuildIndexPayload payload,
        KubeJobContextV2 context,
        CancellationToken cancellationToken)
    {
        // context.RunId + context.Attempt 可用于业务幂等。
        await RebuildShardAsync(payload.IndexName, context.ShardIndex,
            context.TotalShards, cancellationToken);
    }
}
```

## Raw UTF-8 Job

极致热路径可以实现 `IKubeJobV2`，直接读取 `context.PayloadUtf8.Span`，避免 typed payload 反序列化和对象图分配。Typed Job 默认更易用，Raw Job 只应在基准证明必要时使用。

## 旧 API 兼容

现有 `IKubeJob` 会通过兼容适配器运行，因此可以逐个 Job 迁移。兼容路径不支持 typed payload，并会额外创建旧版 Context；新代码优先使用 `IKubeJob<TPayload>`。

## Enqueue

```csharp
var result = await jobs.EnqueueAsync<RebuildIndexJob, RebuildIndexPayload>(
    new("orders-v4"),
    new JobEnqueueOptions
    {
        QueueName = "maintenance",
        Priority = 50,
        IdempotencyKey = $"orders-v4:{businessVersion}",
        PayloadSchemaVersion = 2
    }, cancellationToken);
```

同一 JobSpec + IdempotencyKey：

- Payload hash 相同：返回原 Batch，`IsDuplicate=true`。
- Payload hash 不同：拒绝，避免调用方错误复用键。

## Payload

- Payload 在 `Kj_JobPayloads` 按 Batch 存一次。
- Run 只保存 BatchId 和不可变执行快照。
- 大 Payload 应存对象存储，并传 URI、长度和 checksum。
- 结果列表只保存短摘要；完整日志/产物不应写入 JobRun 热表。

## 取消

```csharp
await jobs.CancelRunAsync(runId, "deployment superseded");
await jobs.CancelBatchAsync(batchId, "user canceled operation");
```

取消是协作式的。Job 必须尊重 CancellationToken；外部不可取消副作用仍需业务幂等。

## 好用性后续项

- `CatchUpPolicy`、`MisfireGracePeriod`
- JobSpec 级 `MaxParallelism`
- queue/tenant 配额与加权公平
- progress/checkpoint API
- OpenTelemetry Activity 与 Metrics
- Dashboard 按 Batch 展示分片进度、重试和版本不兼容原因
