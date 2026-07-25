# Local development stack

KubeJob uses PostgreSQL as its durable source of truth and can optionally use
RabbitMQ for queue wake-up notifications. The repository includes a development
Compose stack that works with Docker Compose, `podman compose`, or
`podman-compose`.

> The included credentials and published ports are for local development only.
> Production deployments must provide their own secrets, networking, TLS,
> persistence, backups, and upgrade policy.

## Prerequisites

- .NET 9 SDK;
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

To configure another application manually:

```bash
export ConnectionStrings__KubeJob="$(bash scripts/dev-stack.sh connection-string)"
dotnet run --project path/to/your-app.csproj
```

RabbitMQ remains optional. Use the AMQP endpoint from the stack when configuring
`UseRabbitMqKubeJobNotifications` and
`AddRabbitMqKubeJobWorkerNotifications`.

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
