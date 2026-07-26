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
        dispatch["IExecutionDispatcher\nExecution Envelope"]
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

The following is a proposed optional mode for workloads such as
`订单推送2` at hundreds or thousands of messages per second. It is different
from the current notification-assisted pull mode: the execution broker, not a
Worker claim scan, delivers the work envelope to a Consumer Group.

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
        outbox["Execution Dispatch Outbox\n待投递任务"]
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
    Ingress->>DB: JobRun Pending + DispatchOutbox
    DB-->>Ingress: run-abc 已提交
    Ingress-->>Order: 业务消息 ACK

    Dispatch->>DB: Claim DispatchOutbox
    Dispatch->>MQ: ExecutionEnvelope(run-abc, orderId=O-1001)
    MQ-->>Dispatch: Publisher confirm
    Dispatch->>DB: 标记已投递

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
| 数据库 Claim | 按队列扫描、`SKIP LOCKED` | 按 DispatchId/RunId 接受消息，避免空轮询和大范围扫描 |
| MQ 消息内容 | 只有唤醒提示 | 执行信封，可包含 Payload 或 PayloadRef |
| 任务状态 | PostgreSQL | 仍由 PostgreSQL 保存 |
| Worker ACK | 唤醒后即可 ACK | Complete 成功后才能 ACK |
| 重试主责 | KubeJob Attempt/Lease | 必须明确由 MQ 或 KubeJob 其中一方主导 |
| 顺序保证 | `ConcurrencyKey` + 数据库锁 | Kafka 使用 `orderId` 分区；其他 MQ 需要业务分片或数据库 fencing |
| 当前代码状态 | 已实现 | 需要新增执行分发 Adapter |

This mode still provides at-least-once execution. An external order channel
must accept an application idempotency key such as `orderId`; neither a broker
ACK nor a PostgreSQL completion transaction can undo a side effect that
already succeeded before a worker crash.

The broker should absorb bursts, while PostgreSQL remains the authoritative
ledger. If PostgreSQL is unavailable, the dispatcher stops advancing its
Outbox and consumers must delay or retry instead of acknowledging messages.
