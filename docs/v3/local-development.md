# Local development

Use the included development stack for PostgreSQL and RabbitMQ. It supports
Docker Compose, `podman compose`, and `podman-compose`; .NET 10 SDK is also
required.

> The published ports and credentials are for local development only. Do not
> use them for a shared or production deployment.

## Start the stack

On macOS or Linux:

```bash
bash scripts/dev-stack.sh up
```

On Windows PowerShell:

```powershell
pwsh scripts/dev-stack.ps1 -Action up
```

The stack exposes PostgreSQL on `localhost:5432`, RabbitMQ AMQP on
`localhost:5672`, and the RabbitMQ management UI on
`http://localhost:15672`. Default development credentials are
`kubejob` / `kubejob-dev`.

## Run the unified sample

The sample starts a unified PostgresManaged host and its dashboard:

```bash
bash scripts/run-unified-sample.sh
```

Open `http://localhost:5041/admin/jobs` after startup. To populate it with
real success, retry, timeout, and cancellation paths, run in a second terminal:

```bash
bash scripts/seed-dashboard-demo.sh
```

## Verify changes

```bash
dotnet test KubeJob.sln -c Release
```

Set `KUBEJOB_RABBITMQ_TEST_CONNECTION` to an AMQP connection string to run the
RabbitMQ integration tests. Without it, those tests are intentionally skipped.

## Operate the stack

```bash
bash scripts/dev-stack.sh status
bash scripts/dev-stack.sh logs
bash scripts/dev-stack.sh stop
bash scripts/dev-stack.sh down
```

`down` preserves local data volumes. To remove all local PostgreSQL and
RabbitMQ data, use this destructive command deliberately:

```bash
bash scripts/dev-stack.sh reset --yes
```
