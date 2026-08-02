# 开源消息系统的有序消费模式对照

本笔记以项目官方文档和公开源码为依据，比较“队列级顺序”和高吞吐如何共存。这里的
**顺序**均是消费/执行顺序，不自动等价于业务最终状态正确；写入端仍应使用幂等键和
版本（或序号）条件更新。

## 对照结论

| 项目 | 顺序边界 | 并行边界 | 失败时的处理及对顺序的影响 |
|---|---|---|---|
| Apache RocketMQ | 一个 `MessageQueue` | 不同 `MessageQueue` 可并行；同队列 one queue/one thread | 顺序监听器失败会暂停并重试当前队列，后续消息不越过失败消息；超过策略后转重试/DLQ 时，是否放行后续必须明确。 |
| Apache Kafka | 一个 partition | partition 数就是组内最大消费并行度；同一 partition 同时只归一个 consumer | 仅在业务处理成功后提交 offset，故崩溃会从已提交 offset 重放。若异步处理或提交超前，会破坏“已完成顺序”的判断。 |
| Temporal | 单个 Workflow 的 history/event 顺序，而非 Task Queue 的严格 FIFO | Worker 有空闲容量才 poll；Task Queue 可分区、可扩展 | 单分区任务队列也只是“几乎 FIFO”；多分区随机分配。可靠顺序状态放进单 Workflow 的确定性历史，而不是依赖共享 Task Queue。 |
| MassTransit | 单个**进程实例**内的 partition key | 分区数限制同实例内的并发；同 key 串行 | 其 `UsePartitioner` 不跨负载均衡实例；即时 retry 与延迟 redelivery 都属于失败路径，不能据此宣称集群级 FIFO。 |

## 1. RocketMQ：最接近“队列级严格顺序”的消费模型

RocketMQ 的 [`MessageListenerOrderly`](https://github.com/apache/rocketmq/blob/develop/client/src/main/java/org/apache/rocketmq/client/consumer/listener/MessageListenerOrderly.java)
源码注释明确为 “One queue by one thread”。在
[`ConsumeMessageOrderlyService`](https://github.com/apache/rocketmq/blob/develop/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessageOrderlyService.java)
中，消费者同时使用本地 `MessageQueueLock` 和 Broker 的队列锁；同一 `MessageQueue`
顺序执行，不同队列仍由消费线程池并行。

处理失败返回 `SUSPEND_CURRENT_QUEUE_A_MOMENT` 时，当前批次被放回本地
`ProcessQueue`，延迟后重试，且本次循环不继续消费后面的消息；成功才提交并更新 offset。
这是“失败阻塞该 lane”的核心。官方的[顺序消息说明](https://rocketmq.apache.org/docs/featureBehavior/03fifomessage/)
也规定后续消息必须等待前一有序消息成功；其[消费重试文档](https://rocketmq.apache.org/docs/4.x/consumer/02push/)
说明有序消费先本地重试以避免跳过。

**可借鉴点：** KubeJob 的 `StrictFifo` 不应只用互斥锁，而要以 `lane lease + 当前序号`
维护一个可恢复的“头部阻塞”状态：当前 Run 成功/明确跳过前，不能 Admit 同 lane 后续 Run。

## 2. Kafka：以固定分区换取并行度

Kafka 仅保证一个 partition 内的全序；相同 key 稳定映射到一个 partition 才能保该 key
的顺序，跨 partition 没有顺序。全局顺序意味着单 partition，也意味着一个 consumer
group 中只有一个消费进程能实际处理它。[官方介绍](https://kafka.apache.org/0102/getting-started/introduction/)
对此有明确说明。

[`KafkaConsumer` Javadoc](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java)
规定一个 group 内一个 partition 同时只分配给一个 consumer，并区分内存消费 position 与
持久 committed offset。进程崩溃从最后提交 offset 恢复；因此关闭自动提交、在业务处理
成功后再提交是保持 at-least-once 与可重放顺序的必要条件。`max.poll.interval.ms` 超时会
引发 rebalance，异步执行还需保证 offset 不越过未完成的前序消息。

**可借鉴点：** 将 `ConcurrencyKey` 的稳定哈希固定到 N 个逻辑 lane。N 是吞吐上限的主要
调节钮；扩缩容不要改变已有 key 的 lane 映射，或必须通过迁移/双写切换完成。

## 3. Temporal：将“实体顺序”下沉到持久化状态机

Temporal 的 [Task Queue 文档](https://docs.temporal.io/task-queue) 明确：Worker 仅在有剩余
容量时 poll，多个 Worker 可负载均衡；Task Queue 默认有 4 个 partition。单分区只是“几乎
FIFO”，多分区中的 task 被随机分配，所以它不提供共享队列的严格 FIFO。相反，同一
Workflow Execution 的 History 事件顺序一旦写入便保持不变。

**可借鉴点：** 对“一个订单/设备状态机必须逐条演进”的场景，KubeJob 应像 Temporal 一样把
顺序的权威放在持久化实体（`PartitionKey + NextSequence`），而非相信 MQ 投递先后。Broker
负责唤醒和搬运，Control Plane 决定谁可执行。

## 4. MassTransit：同键串行是局部能力，集群顺序需运输层配合

MassTransit 的 [`UsePartitioner`](https://masstransit.io/documentation/configuration/middleware/filters/partitioner)
按消息中提取的 key 哈希到固定数量分区，保证同 key 在**单个 bus 实例**顺序处理；其文档
特别声明：该 filter 不会跨 load-balanced consumer instance 分区。`ConcurrentMessageLimit`
则只是 Bus、Endpoint 或 Consumer 层面的总并发上限。

RabbitMQ endpoint 的 `PrefetchCount` 是可同时处理的未确认消息数；官方配置文档给出的
默认值随 CPU 数决定。[MassTransit RabbitMQ 配置](https://masstransit.io/documentation/configuration/transports/rabbitmq)
同时说明批量发送可用 `MessageLimit` 和很短的 `Timeout` 提升吞吐，这与 KubeJob 的“数量或
时间任一满足即 flush”一致。

MassTransit 在异常时可即时 retry、延迟 redelivery，最终默认进入 `_error` 队列；
[官方异常文档](https://masstransit.io/documentation/concepts/exceptions) 说明了这些路径。
因此它的 partitioner 是“同键不并发”的实用工具，而不是跨实例、遇错仍严格不越过的 FIFO
协议。

**可借鉴点：** `ConcurrencyKey` 可以采用类似 partitioner 的本地排队以降低数据库锁竞争，
但必须与持久 `PartitionKey + Sequence` gate 结合，才能在多 Worker/故障接管下保证顺序。

## 对 KubeJob 的落地建议

1. 默认 `Parallel`：保持当前高吞吐路径，微批持久化、多个消费者、受 Worker 槽位约束的
   prefetch；最终写入使用幂等键与版本条件。
2. `KeyOrdered`：`PartitionKey` 稳定映射为 N 个 lane；每 lane 只有一个 active owner，
   lane 内以 `Sequence` Admit，lane 间完全并发。这是 Kafka/RocketMQ 的吞吐模型。
3. `StrictFifo`：`Prefetch=1`、一个 active consumer/lease、失败默认阻塞 lane。只有用户
   明确选择“跳过到 DLQ 后放行”时才推进序号，并把该决定写入 Run/Attempt 历史。
4. 不把 Redis 锁当成顺序协议：锁能防并发，但不能规定多个实例已预取消息的先后；顺序游标、
   owner fencing 和 retry/DLQ 决策都应持久化在 Control Plane。

最终推荐是把“队列级顺序”实施为**一条 logical lane 的顺序**，而不是所有业务共用一条物理
队列。这样既符合 RocketMQ/Kafka 的实践，也能让无关业务键继续横向扩 TPS。
