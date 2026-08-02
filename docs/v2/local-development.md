# Local development stack

KubeJob uses PostgreSQL as its durable source of truth and can optionally use
RabbitMQ for queue wake-up notifications. The repository includes a development
Compose stack that works with Docker Compose, `podman compose`, or
`podman-compose`.

> The included credentials and published ports are for local development only.
> Production deployments must provide their own secrets, networking, TLS,
> persistence, backups, and upgrade policy.

## Prerequisites

- .NET 10 SDK;
- Docker with the Compose plugin, or Podman with a Compose provider.

The scripts prefer Docker when both engines are installed. Set
`KUBEJOB_CONTAINER_ENGINE=podman` to force Podman.

## Start the middleware

macOS or Linux:

```bash
bash scripts/dev-stack.sh up
```

Windows PowerShell:

```powershell
pwsh scripts/dev-stack.ps1 -Action up
```

The command starts and health-checks:

| Service | Default endpoint | Development credentials |
|---|---|---|
| PostgreSQL | `localhost:5432` | database/user `kubejob`, password `kubejob-dev` |
| RabbitMQ AMQP | `localhost:5672` | user `kubejob`, password `kubejob-dev` |
| RabbitMQ management | `http://localhost:15672` | user `kubejob`, password `kubejob-dev` |

Copy `.env.example` to `.env` to override image tags, ports, database names, or
credentials. `.env` is ignored by Git.

## Run the unified sample

The one-command runner starts the middleware, reads the actual mapped
PostgreSQL port and credentials, configures the sample, initializes the schema,
and starts the application:

```bash
bash scripts/run-unified-sample.sh
```

```powershell
pwsh scripts/run-unified-sample.ps1
```

Open `http://localhost:5041/admin/jobs` for the Dashboard. The unified sample
continues to use the in-memory store when no `ConnectionStrings__KubeJob`
setting is supplied, so it remains usable without containers.

## Seed real Dashboard acceptance data

After the unified sample has started, run this in another terminal:

```bash
bash scripts/seed-dashboard-demo.sh
```

```powershell
pwsh scripts/seed-dashboard-demo.ps1
```

The script submits real jobs through the public `IJobClient`, and the Worker
claims and executes them normally:

- a successful first Attempt;
- a transient failure followed by a successful retry;
- retryable failures that exhaust `MaxAttempts` and become `Dead`;
- payload validation that becomes a permanent failure;
- two timed-out Attempts that become `Dead`;
- a long-running `cancel-me` job for testing cooperative cancellation from the Dashboard.

These are not rows inserted directly into the database. Every Run goes through
submission, Claim, Attempt, Lease, retry, and Completion behavior, so the batch
is suitable for validating Jobs, Failures, execution timelines, Worker capacity,
and cancellation. Failure and timeout scenarios take several seconds to settle;
the Dashboard pages refresh automatically.

A log platform or distributed tracing backend is not required. Applications may
optionally use Run IDs, Attempt IDs, or Trace IDs to build links into their own
observability system, while the KubeJob Dashboard remains independently useful.

To configure another application manually:

```bash
export ConnectionStrings__KubeJob="$(bash scripts/dev-stack.sh connection-string)"
dotnet run --project path/to/your-app.csproj
```

RabbitMQ remains optional. The unified sample serves the `sample.data` and
`sample.dashboard-demo` queues, and starts the RabbitMQ Execution Consumer
when `ConnectionStrings__RabbitMQ` is set:

```bash
export ConnectionStrings__RabbitMQ='amqp://kubejob:kubejob-dev@localhost:5672/'
bash scripts/run-unified-sample.sh
```

After startup, the RabbitMQ management page shows the durable per-group
`kubejob.execution.unified-sample` exchange and one durable queue per logical
queue served by the `unified-sample` consumer group. Without the connection
string, the sample still runs and workers claim from the database, but the
default delivery profile is `BrokerDispatch`: with no `rabbitmq` transport
registered, outbox rows cannot be published and retry until a transport is
registered (`UnconfiguredExecutionTransport`). Pointing `ConnectionStrings__RabbitMQ`
at the dev broker is the supported configuration.

## Operations

```bash
bash scripts/dev-stack.sh status
bash scripts/dev-stack.sh logs
bash scripts/dev-stack.sh logs postgres
bash scripts/dev-stack.sh stop
bash scripts/dev-stack.sh down
```

`down` preserves the named data volumes. To remove all local PostgreSQL and
RabbitMQ data, use the explicitly destructive command:

```bash
bash scripts/dev-stack.sh reset --yes
```

PowerShell exposes the same actions; use `-Service postgres` for a single log
stream and `-Yes` for reset.
