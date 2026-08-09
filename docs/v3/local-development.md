# Local development

The repository ships a small PostgreSQL and RabbitMQ stack for development.
The scripts work with Docker Compose, `podman compose`, and
`podman-compose`. The .NET 10 SDK is required for builds and tests.

The credentials and published ports below are for a local machine only.

## Start PostgreSQL and RabbitMQ

If Podman is installed, force the script to use it:

```bash
KUBEJOB_CONTAINER_ENGINE=podman bash scripts/dev-stack.sh up
```

Without the environment variable, the script picks the first working Compose
provider. On Windows PowerShell, use:

```powershell
pwsh scripts/dev-stack.ps1 -Action up
```

When the health checks pass, the script prints the actual connection string
and RabbitMQ management URL. With the default compose file, the services are
available at:

| Service | Address |
| --- | --- |
| PostgreSQL | `localhost:5432` |
| RabbitMQ AMQP | `localhost:5672` |
| RabbitMQ management UI | <http://localhost:15672> |
| RabbitMQ credentials | `kubejob` / `kubejob-dev` |

## Run the sample

In another terminal:

```bash
bash scripts/run-unified-sample.sh
```

The unified sample runs PostgresManaged jobs and initializes the PostgreSQL
schema when a connection string is available. Open
<http://localhost:5041/admin/jobs> for the dashboard.

To create representative rows for the dashboard, run:

```bash
bash scripts/seed-dashboard-demo.sh
```

The seed command creates success, retry, timeout, and cancellation scenarios;
it is safe to run again because each batch uses a new idempotency prefix.

## Run tests

```bash
dotnet test KubeJob.sln -c Release
```

The RabbitMQ integration tests are skipped unless
`KUBEJOB_RABBITMQ_TEST_CONNECTION` contains an AMQP connection string. This
keeps the normal test run independent of a local broker.

## Inspect and stop the stack

```bash
bash scripts/dev-stack.sh status
bash scripts/dev-stack.sh logs
bash scripts/dev-stack.sh stop
bash scripts/dev-stack.sh down
```

`down` removes the containers but keeps the named data volumes. To remove all
local PostgreSQL and RabbitMQ data, run the destructive command explicitly:

```bash
bash scripts/dev-stack.sh reset --yes
```
