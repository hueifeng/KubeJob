# 本地开发环境

KubeJob 使用 PostgreSQL 作为持久化事实来源，并可选使用 RabbitMQ 提供 Queue
唤醒通知。仓库现在提供一套开发用 Compose 环境，可由 Docker Compose、
`podman compose` 或 `podman-compose` 启动。

> 内置账号、密码和端口只适用于本地开发。生产环境必须自行配置 Secret、网络、
> TLS、持久化、备份和升级策略。

## 前置条件

- .NET 10 SDK；
- Docker + Compose 插件，或安装了 Compose Provider 的 Podman。

同时安装 Docker 和 Podman 时，脚本默认优先 Docker。可设置
`KUBEJOB_CONTAINER_ENGINE=podman` 强制使用 Podman。

## 启动中间件

macOS 或 Linux：

```bash
bash scripts/dev-stack.sh up
```

Windows PowerShell：

```powershell
pwsh scripts/dev-stack.ps1 -Action up
```

脚本会启动并等待以下服务通过健康检查：

| 服务 | 默认地址 | 开发账号 |
|---|---|---|
| PostgreSQL | `localhost:5432` | 数据库/用户 `kubejob`，密码 `kubejob-dev` |
| RabbitMQ AMQP | `localhost:5672` | 用户 `kubejob`，密码 `kubejob-dev` |
| RabbitMQ 管理页面 | `http://localhost:15672` | 用户 `kubejob`，密码 `kubejob-dev` |

需要修改镜像版本、端口、数据库名或账号时，将 `.env.example` 复制为 `.env`。
`.env` 已被 Git 忽略。

## 一条命令运行统一示例

下面的脚本会启动中间件、读取实际映射的 PostgreSQL 端口和账号、配置示例、
初始化数据库 Schema，并启动应用：

```bash
bash scripts/run-unified-sample.sh
```

```powershell
pwsh scripts/run-unified-sample.ps1
```

Dashboard 地址为 `http://localhost:5041/admin/jobs`。如果没有提供
`ConnectionStrings__KubeJob`，统一示例仍会回退到内存存储，因此不安装容器也能运行。

## 生成真实 Dashboard 验收数据

统一示例启动后，在另一个终端执行：

```bash
bash scripts/seed-dashboard-demo.sh
```

```powershell
pwsh scripts/seed-dashboard-demo.ps1
```

脚本通过公开的 `IJobClient` 提交一组真实任务，Worker 会正常领取和执行它们：

- 一次执行成功；
- 第一次失败、第二次重试成功；
- 可重试异常耗尽 `MaxAttempts` 后进入 `Dead`；
- Payload 校验异常直接进入永久失败；
- 两次执行超时后进入 `Dead`；
- 一个长时间运行的 `cancel-me` 任务，可在 Dashboard 中测试协作式取消。

这些记录不是直接写入数据库的演示数据。它们完整经过提交、Claim、Attempt、Lease、
重试和 Completion 流程，因此适合验收 Jobs、Failures、执行时间线、Worker 容量和取消操作。
失败与超时场景大约需要数秒完成；页面会自动刷新。

日志平台和分布式 Trace 不是运行该验收流程的前置条件。应用可选择在自己的日志系统中
使用 Run ID、Attempt ID 或 Trace ID 建立跳转，但 KubeJob Dashboard 在没有这些集成时也能独立工作。

手动配置其他应用：

```bash
export ConnectionStrings__KubeJob="$(bash scripts/dev-stack.sh connection-string)"
dotnet run --project path/to/your-app.csproj
```

RabbitMQ 仍然是可选加速层。统一示例服务 `sample.data` 与
`sample.dashboard-demo` 两个队列，设置 `ConnectionStrings__RabbitMQ` 后
启动 RabbitMQ Execution Consumer：

```bash
export ConnectionStrings__RabbitMQ='amqp://kubejob:kubejob-dev@localhost:5672/'
bash scripts/run-unified-sample.sh
```

启动后，RabbitMQ 管理页面会出现按组命名的 `kubejob.execution.unified-sample`
exchange，以及按 `unified-sample` Consumer Group 和逻辑队列创建的持久化队列。
未配置该连接串时示例仍可运行、Worker 照常从数据库领取任务，但默认投递档已是
`BrokerDispatch`：没有注册 `rabbitmq` transport 时 outbox 行无法发布、会持续重试
（`UnconfiguredExecutionTransport`）。把 `ConnectionStrings__RabbitMQ` 指向开发 broker
才是受支持的配置。

## 常用操作

```bash
bash scripts/dev-stack.sh status
bash scripts/dev-stack.sh logs
bash scripts/dev-stack.sh logs postgres
bash scripts/dev-stack.sh stop
bash scripts/dev-stack.sh down
```

`down` 会保留命名数据卷。需要彻底删除本地 PostgreSQL 和 RabbitMQ 数据时，必须显式执行：

```bash
bash scripts/dev-stack.sh reset --yes
```

PowerShell 提供相同 Action；单服务日志使用 `-Service postgres`，重置数据使用 `-Yes`。
