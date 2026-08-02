# KubeJob Logical Architecture

This document describes the runtime as a set of deep modules and adapters.
The durable state machine lives behind the control-plane interfaces; HTTP,
typed clients, Dashboard actions, in-process execution, and message brokers
are adapters at those seams.

## 0. Concrete example: `订单推送2`

Imagine an order system publishing an `OrderPaid` event. The business meaning
is “push this order to the external channel”; KubeJob's job type is
`order-push-2`.

```mermaid
flowchart LR
    subgraph Business["业务系统"]
        order["订单系统"]
        event["OrderPaid\norderId=O-1001\nmessageId=evt-9001"]
        order --> event
    end

    subgraph MQ["消息中间件"]
        exchange[("orders.events\nRabbitMQ / Kafka topic")]
    end

    subgraph KubeJobCP["KubeJob 控制面"]
        ingress["消息接入 Adapter\nIJobMessageIngress"]
        submit["JobControlPlane\njobKey = order-push-2"]
        run["JobRun\nPending\nidempotency = orders:evt-9001"]
        db[("PostgreSQL\nRun + Outbox")]
    end

    subgraph KubeJobWorker["KubeJob Worker"]
        claim["WorkerControlPlane\nqueue = orders\ncapability = order-push-2"]
        handler["OrderPush2Handler\n调用外部渠道"]
    end

    subgraph Console["Dashboard"]
        view["查看 Run / Attempt\n成功、重试、失败"]
    end

    event --> exchange --> ingress --> submit --> run --> db
    db --> claim --> handler
    handler --> claim
    db --> view
    exchange -. "不是直接投递执行权" .-> claim
```

这条链路中，订单系统只负责发布业务事件；消息接入 Adapter 把事件转
换成一次 KubeJob 提交。Worker 不需要直接订阅订单消息，而是向控制面
注册能力并 Pull/Claim `order-push-2` 任务。这样队列匹配、并发、租约、
重试和过期 Worker 防护都集中在控制面。

| 字段 | 订单推送2示例 | 作用 |
| --- | --- | --- |
| Job key | `order-push-2` | 标识执行哪一种 Handler |
| Queue | `orders` | 限制哪些 Worker 可以领取 |
| Message ID | `evt-9001` | 用于消息重复投递幂等 |
| Idempotency key | `orders:evt-9001` | 确保只产生一个 JobRun |
| Concurrency key | `order:O-1001` | 同一订单避免并发推送 |
| JobRun | `run-abc` | 这次订单推送的逻辑任务 |
| JobAttempt | Attempt 1, 2... | 每次实际执行和重试 |

```mermaid
sequenceDiagram
    autonumber
    participant Order as 订单系统
    participant Broker as MQ
    participant Ingress as 订单消息接入
    participant CP as JobControlPlane
    participant DB as PostgreSQL
    participant Worker as Worker
    participant Channel as 外部渠道

    Order->>Broker: 发布 OrderPaid(evt-9001)
    Broker->>Ingress: 投递 evt-9001
    Ingress->>CP: Submit(order-push-2, idempotency=orders:evt-9001)
    CP->>DB: 写入 JobRun Pending + Outbox
    DB-->>CP: run-abc, Existing=false
    CP-->>Ingress: 已持久化
    Ingress-->>Broker: ACK

    Worker->>CP: Claim(queue=orders, capability=order-push-2)
    CP->>DB: 校验 Session + 容量 + Lease
    DB-->>CP: Attempt 1
    CP-->>Worker: 返回订单 Payload
    Worker->>Channel: 推送订单 O-1001
    Channel-->>Worker: 超时 / 失败
    Worker->>CP: Complete(Attempt 1, retryable)
    CP->>DB: Attempt 1 结束，Run 回到 Pending
    Worker->>CP: 再次 Claim
    CP-->>Worker: Attempt 2
    Worker->>Channel: 再次推送订单
    Channel-->>Worker: 成功
    Worker->>CP: Complete(Attempt 2, succeeded)
    CP->>DB: Run = Succeeded

    Broker->>Ingress: 重复投递 evt-9001
    Ingress->>CP: 相同 idempotency key
    CP->>DB: 找到 run-abc
    DB-->>CP: Existing=true
    Ingress-->>Broker: ACK，不创建第二个 Run
```

如果达到最大尝试次数，`run-abc` 会进入 `Dead`，Dashboard 可以直接看到
失败原因和每一次 Attempt；如果 Worker 中途宕机，LeaseReaper 会回收
Attempt，之后由其他健康 Worker 重新 Claim。

## 1. Runtime topology

```mermaid
flowchart LR
    subgraph Sources["Submission and operator sources"]
        typed["IJobClient / IJobScheduleClient"]
        http["HTTP jobs and schedules API"]
        runtimeHttp["HTTP worker runtime API"]
        dashboard["Dashboard actions"]
        dashboardRead["DashboardCatalogReader\nread-only projections"]
        ingress["Business-message ingress adapter\nRabbitMQ today; Kafka/NATS/etc. later"]
    end

    subgraph CP["KubeJob.ControlPlane"]
        jobs["JobControlPlane\nsubmit, query, cancel, idempotency"]
        schedules["ScheduleControlPlane\ncron, time zone, lifecycle"]
        workers["WorkerControlPlane\nregister, claim, renew, complete"]
        message["IJobMessageIngress\nmessage identity -> idempotent submit"]
        reconcilers["Reconciliation services\nOutbox publisher, lease reaper, schedule reconciler"]
    end

    subgraph State["Authoritative state adapters"]
        postgres[("PostgreSQL")]
        memory[("In-memory store\nlocal/dev/test")]
    end

    subgraph Signals["Optional acceleration"]
        outbox[("Kj2_Outbox")]
        wakeup["IWorkAvailableNotifier\ncoalesced wake-up hint"]
        dispatch["IExecutionTransport\nExecution Envelope"]
    end

    subgraph Workers["Worker runtime"]
        worker["WorkerRuntimeService\nbounded local execution"]
        trigger["IWorkerClaimTrigger\nwake now or periodic poll"]
        handlers["Typed job handlers"]
    end

    typed --> jobs
    typed --> schedules
    http --> jobs
    http --> schedules
    runtimeHttp --> workers
    dashboard --> jobs
    dashboard --> schedules
    dashboardRead --> postgres
    dashboardRead --> memory
    ingress --> message --> jobs

    jobs --> postgres
    schedules --> postgres
    workers --> postgres
    reconcilers --> postgres
    jobs --> outbox
    schedules --> outbox
    outbox --> wakeup --> trigger
    outbox -. "BrokerDispatch profile" .-> dispatch
    trigger --> worker --> workers
    dispatch -. "RabbitMQ/Kafka" .-> worker
    worker --> handlers
    memory -. "alternate adapter" .-> jobs
    memory -. "alternate adapter" .-> schedules
    memory -. "alternate adapter" .-> workers
```

### Ownership rules

| Module or adapter | Owns | Does not own |
| --- | --- | --- |
| `JobControlPlane` | submission validation, idempotency command creation, public run snapshots | HTTP status codes, broker acknowledgements |
| `ScheduleControlPlane` | cron validation, time-zone calculation, schedule lifecycle | timer ownership, HTTP serialization |
| `WorkerControlPlane` | session fencing, claim limits, lease renewal, completion rules | handler execution, broker consumption |
| `IJobMessageIngress` adapter | converting a broker envelope into one durable submission | execution ownership, retry policy outside KubeJob |
| `IWorkAvailableNotifier` adapter | reducing claim latency with a wake-up hint | correctness, durable job state |
| worker runtime | bounded execution, handler invocation, heartbeat and completion calls | global scheduling decisions |
| storage adapter | transactions, locking, durable state and outbox writes | transport protocol and UI rendering |

The control-plane modules are the deep modules. Their interfaces are small
relative to the validation, fencing, retry, and snapshot behavior behind them.
The storage interfaces are the real varying seam: the in-memory and PostgreSQL
adapters make that seam useful in tests and deployments.

## 2. Submit and execute a job

```mermaid
sequenceDiagram
    autonumber
    participant Caller as Caller / HTTP / Dashboard
    participant Jobs as JobControlPlane
    participant Store as Storage adapter
    participant DB as PostgreSQL
    participant Outbox as Outbox publisher
    participant Worker as WorkerRuntimeService
    participant Handler as Typed handler

    Caller->>Jobs: Submit(EnqueueJobRequest)
    Jobs->>Jobs: Validate payload, limits, queue
    Jobs->>Store: Submit(command with idempotency key)
    Store->>DB: Transaction: Run + Outbox
    DB-->>Store: Run id / existing identity
    Store-->>Jobs: JobSubmissionReceipt
    Jobs-->>Caller: JobHandle + Existing

    Outbox->>DB: Read pending notification
    Outbox-->>Worker: Wake-up hint (optional)
    Worker->>Jobs: Register / Claim
    Jobs->>Store: Fenced claim transaction
    Store->>DB: Session check + capacity + lease
    DB-->>Store: Claimed Attempt
    Store-->>Jobs: Claimed job
    Jobs-->>Worker: Payload-free claim metadata + payload
    Worker->>Handler: Execute(payload)
    Worker->>Jobs: Complete(attempt, lease token, result)
    Jobs->>Store: Fenced completion
    Store->>DB: Attempt + Run transition
    DB-->>Store: Accepted / rejected as stale
    Store-->>Jobs: Completion result
    Jobs-->>Worker: Completion result
```

The worker is a pull client. A wake-up signal only asks it to claim sooner;
periodic polling remains the correctness path. A broker message is different:
it is a business submission and is acknowledged only after the durable Run is
accepted.

## 3. Business-message ingress acknowledgement

```mermaid
sequenceDiagram
    participant Broker as RabbitMQ / future MQ adapter
    participant Ingress as IJobMessageIngress adapter
    participant Jobs as JobControlPlane
    participant Store as Storage adapter

    Broker->>Ingress: Deliver(messageId, job envelope)
    Ingress->>Jobs: Submit(messageId-scoped idempotency key)
    Jobs->>Store: Insert Run + Outbox atomically
    alt first delivery
        Store-->>Jobs: accepted, Existing = false
    else redelivery or duplicate
        Store-->>Jobs: existing Run, Existing = true
    end
    Jobs-->>Ingress: durable acceptance
    Ingress-->>Broker: ACK / commit offset

    Note over Ingress,Broker: Invalid payload -> reject / dead letter\nTransient storage failure -> requeue / retry
```

The adapter never claims a worker slot and never treats broker delivery as
execution completion. The Run remains visible in KubeJob even if a worker is
temporarily unavailable.

## 4. State transitions and fencing

```mermaid
flowchart TD
    pending["Run: Pending"] --> claimed["Attempt leased"]
    claimed --> running["Attempt: Running"]
    running --> succeeded["Run: Succeeded"]
    running --> retry["Retryable failure"]
    retry --> pending
    running --> dead["Run: Dead"]
    claimed --> leaseLost["Lease expired"]
    leaseLost --> pending
    pending --> cancelRequested["Cancel requested"]
    cancelRequested --> canceled["Run: Canceled"]

    fence["WorkerId + SessionId + Epoch + LeaseToken"] -. "must match" .-> claimed
    fence -. "must match" .-> running
    fence -. "must match" .-> succeeded
```

`JobRun` is the logical request and survives retries. `JobAttempt` is one
physical execution. Worker sessions and lease tokens prevent a stale process
from completing work after a restart or lease takeover.

## 5. Deployment choices

### Unified process

```text
ASP.NET host
├── HTTP API + Dashboard adapters
├── KubeJob.ControlPlane
├── in-memory or PostgreSQL storage adapter
├── optional Outbox notifier adapter
└── in-process WorkerRuntimeService
```

The in-process worker uses the same `WorkerControlPlane` as a remote worker;
only the transport adapter changes.

### Distributed deployment

```text
API replicas ───────────────┐
Dashboard operators ────────┼──> Control-plane replicas ───> PostgreSQL
RabbitMQ/Kafka ingress ─────┘                 ▲
                                              │ HTTP pull / renew / complete
Worker replicas ─────────────────────────────┘
```

Multiple control-plane replicas are safe because claiming, fencing, retry, and
schedule occurrence creation are decided by database transactions. A message
broker can accelerate delivery, but it is not the source of truth.

## 6. Why this shape is intentionally not a push dispatcher

The durable state machine and the worker execution engine have different
failure modes. Keeping workers as pull clients gives the control plane one
place to enforce capacity, queue matching, concurrency keys, leases, retries,
and stale-session fencing. Message middleware is therefore an ingress or
wake-up adapter, not a second scheduler hidden inside each broker consumer.

This keeps the external interface small while allowing RabbitMQ, Kafka, NATS,
or another transport to be added as independent adapters later.

## 7. High-throughput execution dispatch mode

This mode is now the default execution delivery profile
(`QueueDeliveryOptions.Defaults.Profile = ExecutionDeliveryProfile.BrokerDispatch`,
see [ADR 014](../adr/014-promote-brokerdispatch-to-default-delivery-profile.md)).
Unlike the notification-assisted pull mode,
the execution broker — not a Worker claim scan — delivers the work envelope to
a Consumer Group. It remains a per-queue delivery profile
(`ExecutionDeliveryProfile.BrokerDispatch`) that an operator can pin back to
`Pull` for any individual queue via a per-queue `QueueDefinition`; the control plane still
owns the durable state machine and PostgreSQL remains the authoritative
ledger.

```mermaid
flowchart LR
    subgraph Business["业务生产端"]
        order["订单系统\n500 orders/s"]
        event["OrderPaid\npartition key = orderId"]
        order --> event
    end

    subgraph Ingress["业务接入"]
        ingressTopic[("orders.events")]
        ingressConsumer["Ingress Adapter\n提交 JobRun"]
    end

    subgraph Control["KubeJob Control Plane"]
        submit["JobControlPlane\n幂等、校验、取消"]
        state[("PostgreSQL\nJobRun / JobAttempt / Lease")]
        outbox["Kj2_Outbox\n待投递 work-available 行"]
        dispatcher["Dispatch Publisher\n发布确认"]
        complete["Complete / Retry / Fence\n状态机"]
    end

    subgraph Execution["执行消息中间件"]
        executionTopic[("orders.push\nTopic / Queue")]
        group["Consumer Group\n按 orderId 分区"]
    end

    subgraph Worker["高吞吐 Worker 集群"]
        consumer["Execution Consumer\nStart/Accept delivery"]
        handler["OrderPush2Handler\n调用外部渠道"]
        consumer --> handler
    end

    subgraph Console["Dashboard"]
        dashboard["Run / Attempt / Backlog / Retry"]
    end

    event --> ingressTopic --> ingressConsumer --> submit
    submit --> state
    state --> outbox --> dispatcher --> executionTopic --> group --> consumer
    consumer --> complete --> state
    state --> dashboard

    ingressConsumer -. "Run + Outbox 提交成功后 ACK" .-> ingressTopic
    consumer -. "Complete 持久化成功后 ACK" .-> executionTopic
```

### `订单推送2` 的一次执行

```mermaid
sequenceDiagram
    autonumber
    participant Order as 订单系统
    participant Ingress as Ingress Consumer
    participant DB as PostgreSQL
    participant Dispatch as Dispatch Publisher
    participant MQ as orders.push
    participant Worker as Worker Consumer
    participant Channel as 外部渠道

    Order->>Ingress: OrderPaid(O-1001, event-9001)
    Ingress->>DB: JobRun Pending + work-available outbox 行
    DB-->>Ingress: run-abc 已提交
    Ingress-->>Order: 业务消息 ACK

    Dispatch->>DB: 读取 work-available outbox 行
    Dispatch->>MQ: ExecutionEnvelope(run-abc, queue=orders.push)
    MQ-->>Dispatch: Publisher confirm
    Dispatch->>DB: 标记已发布

    MQ->>Worker: 投递 ExecutionEnvelope
    Worker->>DB: Start/Accept delivery，创建或确认 Attempt + Lease
    DB-->>Worker: lease-token
    Worker->>Channel: 推送订单 O-1001
    Channel-->>Worker: 成功
    Worker->>DB: Complete(run-abc, attempt, lease-token)
    DB-->>Worker: Succeeded
    Worker-->>MQ: ACK

    Note over MQ,Worker: Worker 崩溃或 ACK 前断开 -> MQ 重投\nStart/Complete 必须幂等
```

### 这个模式与当前 Pull 模式的差异

| 事项 | 当前 Pull 模式 | 高吞吐 Dispatch 模式 |
| --- | --- | --- |
| 任务来源 | Worker 向控制面 Claim | Worker Consumer 从 MQ 收取 |
| 数据库 Claim | 按队列扫描、`SKIP LOCKED` | 按 RunId 接受消息（targeted admission），避免空轮询和大范围扫描 |
| MQ 消息内容 | 只有唤醒提示 | 执行信封（`RunId`/`Queue`/`EventId`，不含 Payload）；worker 凭 `RunId` 向控制面 admission 取 payload |
| 任务状态 | PostgreSQL | 仍由 PostgreSQL 保存 |
| Worker ACK | 唤醒后即可 ACK | Complete 成功后才能 ACK |
| 重试主责 | KubeJob Attempt/Lease | KubeJob `MaxAttempts` 权威；broker `x-delivery-limit` 仅作兜底，触限后进 DLQ |
| 顺序保证 | `ConcurrencyKey` + 数据库锁 | RabbitMQ 用 logical queue 作 routing key，由 `ConcurrencyKey` + 数据库 fencing 保序；Kafka 适配器未来可按 `orderId` 分区 |
| 当前代码状态 | 已实现（默认 profile，可按队列 `QueueDefinition` 覆盖回退） | 已实现（默认 profile；`BrokerCancelPropagationEnabled` 默认 `true`，只 gate broker 取消传播） |

`WorkerRuntimeService.ClaimLoopAsync` 无论 profile 都持续运行，作为 broker 不可用时的存活兜底（见 [ADR 007](../adr/007-mq-notifications-do-not-own-jobs.md)）。`BrokerDispatch` 成为默认后，该轮询在稳态下只是兜底而非主投递路径；纯 `BrokerDispatch` 工作负载的部署应调高 `KubeJobWorkerOptions.EmptyPollDelay`（默认 1s）以降低稳态空轮询频率。

This mode still provides at-least-once execution. An external order channel
must accept an application idempotency key such as `orderId`; neither a broker
ACK nor a PostgreSQL completion transaction can undo a side effect that
already succeeded before a worker crash.

The broker should absorb bursts, while PostgreSQL remains the authoritative
ledger. If PostgreSQL is unavailable, the dispatcher stops advancing its Outbox
and consumers must delay or retry instead of acknowledging messages.

### Direct Dispatch 拓扑与约定

- **Quorum queue 必选。** Direct Dispatch 消费队列以 `x-queue-type=quorum` 声明，broker 持久化 `x-delivery-count`，跨 worker 重启不丢。`x-delivery-limit` 默认关闭；只有部署同时提供 Pending Run 的 DLQ re-drive/reconciliation 时才应显式启用。`RabbitMqTopologyProvisioner`（IHostedService）在 worker 启动时声明队列与 DLX，`RabbitMqExecutionConsumerService` 只做 passive 声明确认队列存在后消费，避免与 quorum 声明冲突。
- **命名约定。** 调度 group exchange `kubejob.execution.{group}`、按业务 logical queue 的调度队列 `kubejob.execution.{group}.{logical-queue}.queue`、group 共享 retry queue `kubejob.execution.{group}.retry.queue`、调度 DLX `kubejob.execution.{group}.dlx`、调度 DLQ `kubejob.execution.{group}.dlq.queue`、取消 fanout exchange `kubejob.execution.{group}.cancel`、每个 Worker 的取消队列 `kubejob.execution.{group}.cancel.{worker-id}`（名字来自稳定 WorkerId，auto-delete，重启复用同名队列、不产生新队列名；Worker 断开后自动删除，不会累积）。前缀 `kubejob.execution` 来自 `RabbitMqExecutionOptions.ConsumerQueuePrefix`（可配），`{group}` 来自 `ConsumerGroup`。每个业务 logical queue 只对应一条可直接识别的物理 dispatch queue，不附加 hash；retry/DLQ 是 group 级技术队列。物理队列名全部稳定可预期，只有取消队列是临时的。
- **Header 约定。** Dispatch envelope 携带 `properties.Type = "execution-envelope"`、`MessageId = EventId`；cancel marker 携带 `properties.Type = "cancel"` 与 `X-KubeJob-Event-Type = "cancel"`，consumer 不用解析 body 即可按 header 分发。
- **投递路径.** `RabbitMqTopologyProvisioner`（IHostedService）启动时声明 quorum 队列 + TTL retry exchange/queue + DLX + 可选 `x-delivery-limit` + 绑定，`RabbitMqExecutionDispatcher` 发布到 group exchange，`RabbitMqExecutionConsumerService` 只对消费队列做 passive 声明确认存在后消费（拓扑不匹配时 fail-fast，而不是 406 无限重连）。每次提交都会写 `work-available` outbox 行；`Pull` profile 发布为 `IWorkAvailableNotifier` 提示（默认 no-op，worker 仍周期性轮询控制面），`BrokerDispatch` profile 则由 `OutboxPublisherService` 转成 `ExecutionEnvelope` 后投递到选定 transport。
- **批量 admission。** 消费者把最多 `AdmissionBatchSize`（默认 16）个信封收集成一批，用一次 `AdmitBatchAsync` claim 事务完成 admission（每批约 2 次数据库往返：一次 claim、一次未 claim 信封的批量诊断），单信封的 ACK/Reject/Retry 语义不变，容量/围栏/排序 gate 仍按 Run 逐一判定。`PrefetchCount` 应不小于 `AdmissionBatchSize` 以保证批次能装满。
- **Opt-in 开关。** `JobRuntimeOptions.BrokerCancelPropagationEnabled` 默认 `true`（随 `BrokerDispatch` 成为默认 profile 一并调整，见 [ADR 014](../adr/014-promote-brokerdispatch-to-default-delivery-profile.md)），只 gate 取消传播：开启时取消 `BrokerDispatch` Run 会解析 consumer group 并写 `cancel` outbox 行，由 `ICancelPublisher` fanout；关闭时取消只置 `CancelRequested`，靠 lease reaper 兜底。未注册 RabbitMQ execution dispatcher 扩展（即无 `ICancelPublisher`）且仍使用 `BrokerDispatch` 的部署应显式将其设回 `false`。投递本身由队列 profile（`BrokerDispatch`）决定，与该 flag 无关。
