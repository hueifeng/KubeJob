# 消息顺序与吞吐：成熟中间件的共同设计

本说明只引用产品官方文档，目的是为 KubeJob 的 BrokerDispatch、Ingress 和
`ConcurrencyKey` 明确顺序语义。结论很一致：**不要默认承诺全局 FIFO；把顺序限定
到业务实体键（partition/group key），并用业务版本号兜底。**

## 先定义需要的语义

| 语义 | 适用场景 | 吞吐代价 |
|---|---|---|
| 无顺序 | 邮件、图片、独立 webhook | 最低，可完全并行 |
| 同键不并发 | 同订单/账户避免同时写入 | 很低，但不等于严格先后顺序 |
| 同键严格有序 | 状态转换、余额、实体版本更新 | 该键在前一条完成/确认前必须阻塞 |
| 全局严格有序 | 极少数单一流水账本 | 整个队列退化为一个并行度，通常不可接受 |

即使中间件提供 FIFO，消费者重试、宕机、超时重投和多个生产者仍使“最终写入”
不能只依赖投递顺序。因此状态更新应携带业务版本：

```sql
UPDATE orders
SET status = @status, version = @new_version
WHERE id = @id AND version < @new_version;
```

若事件绝不能跳号，则将条件改为 `version = @expected_version`，把缺失事件留待重试或补偿。

## 官方产品如何取舍

| 产品 | 顺序边界 | 并行方式 | 对 KubeJob 的启示 |
|---|---|---|---|
| [Apache Kafka](https://kafka.apache.org/documentation/#semantics) | 单个 partition 的日志顺序；同一 key 应稳定映射到同一 partition | 多 partition / consumer group | `ConcurrencyKey`（或明确的 `PartitionKey`）是自然的分片键；不能要求跨 partition 顺序。Producer 可启用 [幂等写入](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/ProducerConfig.html) 降低生产重试重复，但这不替代业务端幂等。 |
| [RabbitMQ](https://www.rabbitmq.com/docs/queues#message-ordering) | 单队列按入队顺序投递；多 channel/connection 发布会交错 | 多 consumer、prefetch；默认 round-robin | 一条物理队列配多个消费者不提供同键严格顺序。RabbitMQ 的 [Single Active Consumer](https://www.rabbitmq.com/docs/consumers#single-active-consumer) 能保整队列处理顺序并自动故障切换，但会牺牲并行度，只适合少数低吞吐严格 FIFO 队列。 |
| [Amazon SQS FIFO](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html) | `MessageGroupId` 内严格顺序；同组前一条 delete/visibility 到期前不再发下一条 | 不同 group 并行 | 最清晰的参考模型：一个业务实体一个 group。`MessageDeduplicationId` 支持生产端重试去重，但仍应让消费者幂等。 |
| [NATS JetStream](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers) | Stream 有序；消费者确认/重投属于 at-least-once 工作模型 | Durable pull consumer 可水平扩展 | 将“有序回放/观察”的 [ordered consumer](https://docs.nats.io/nats-concepts/jetstream/consumers) 与可靠任务消费区分开；任务执行应使用 durable consumer、显式 ACK 和业务去重，而不是把 ordered consumer 当作工作队列的正确性机制。 |

RabbitMQ 还明确说明：提高消费者数、缩短处理时间或提高 prefetch 都可能提高消费能力；这适用于**无顺序或跨键并行**，不能与单队列严格 FIFO 同时获得。[官方消费者指南](https://www.rabbitmq.com/docs/consumers#consumer-capacity)

## 对 KubeJob 的设计建议

### 1. 默认保持无顺序、至少一次和幂等

默认 BrokerDispatch 应允许并发消费和微批提交。MQ 的 ACK 只表示 KubeJob 已持久化
接收（Ingress）或已由控制面收口（Execution）；数据库的 Run/Attempt、租约与 fencing
仍是权威。这与现有 Outbox 设计一致。

### 2. 将 `ConcurrencyKey` 明确为“同键不并发”

不要将它宣传为严格 FIFO。它可以防止同订单的两个 Attempt 同时执行，却无法独自证明：

- 两条消息被多个生产者发出时的先后；
- 旧消息在超时重试后不会晚到；
- 优先级、延迟重试、重发布不会改变交付次序。

### 3. 新增可选的 `PartitionKey + Sequence` 严格有序契约

仅为有因果依赖的 Job 增加可选字段：

```text
PartitionKey = "order:O-1001"
Sequence     = 42
```

控制面以 `PartitionKey` 维护每个键的下一可执行序号；只有该序号的 Run 可被 Admit。
完成成功后推进序号；失败/取消按明确策略阻塞、跳过或补偿。物理投递可按
`hash(PartitionKey)` 分到固定数量的 lane：同 lane 单消费者，不同 lane 并行。这样实现
SQS FIFO message group 的模式，同时不把 PostgreSQL 状态机交给 RabbitMQ。

`Sequence` 应来自拥有该实体写入顺序的业务源（例如订单表版本或事件序号）；KubeJob
不应替多个独立生产者猜测真实发生顺序。

### 4. RabbitMQ 的现实落地

- 常规高 TPS 队列：允许多个 consumer，设置有界 prefetch 与 Worker 槽位一致；按背压
  调整，监控 consumer capacity、未确认数、完成 P95 和 Outbox lag。
- 需要同键顺序：先在控制面做 `PartitionKey + Sequence` gate；可按 hash 路由至多个
  queue/lane，但不要仅凭 routing key 假设同键严格完成顺序。RabbitMQ 如需由 Broker
  做稳定分片，可参考官方的 [`x-modulus-hash` exchange](https://www.rabbitmq.com/docs/modulus-hash-exchange)：
  以 routing key 将实体稳定散列到固定数量的队列，每个队列配 SAC；变更分片数会改变
  映射，因此必须作为迁移操作处理。
- 需要整队列 FIFO：单独物理队列并启用 SAC；接受其单 lane 吞吐上限，不能与通用
  高 TPS 队列混用。

## 结论

对于“同一订单的旧 update 不得覆盖新 update”，首选组合是：

```text
业务实体 Version / Sequence（最终正确性）
    + ConcurrencyKey（防止同时执行）
    + 可选 PartitionKey + Sequence gate（确需严格按序处理时）
```

这保留不同订单间的并行度，也使重复、延迟和故障恢复不会破坏最终状态。
