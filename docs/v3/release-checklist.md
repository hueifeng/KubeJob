# Release checklist

Use this list before tagging a release. It is intentionally short; detailed
behavior belongs in the user-facing guides.

## Runtime

- [x] PostgresManaged keeps `JobRun`, attempt, lease, and completion state in
  PostgreSQL.
- [x] BrokerNative does not create managed runs or leases.
- [x] A logical queue is routed to one execution model.
- [x] Managed claims, renewals, and completions enforce `FenceVersion` and
  lease-token fencing.
- [x] Completion intents are persisted before worker completion batching and
  stale intents are cleaned up.
- [x] Timeout scanning reconciles attempts that remain running past
  `TimeoutSeconds`.
- [x] RetryPolicy is propagated through managed, scheduled, and BrokerNative
  execution paths.
- [x] Transport adapters declare the capabilities that their queues require.
- [x] In-process and HTTP submission use the configured queue route.

## Tests and packaging

- [x] `dotnet test KubeJob.sln -c Release` passes.
- [x] RabbitMQ integration tests run when
  `KUBEJOB_RABBITMQ_TEST_CONNECTION` is provided.
- [x] The package contains `README.md` and `LICENSE`.
- [x] Benchmark code stays under `tests/KubeJob.Benchmark` and is not referenced
  by runtime libraries.

## Repository hygiene

- [x] Obsolete V2 guides, orphan samples, generated locks, and review reports
  are removed.
- [x] Temporary benchmark output is ignored.
- [x] Duplicate validation workflows and unused runtime helpers are removed.

## Documentation

- [x] README links to the current quick start, local development guide,
  runtime choice, event subscriptions, and license.
- [x] Every current guide names its prerequisites and failure semantics.
- [x] Runtime docs describe fencing, completion recovery, timeout scanning,
  retry policy, and ExecutionContext cancellation requirements.
- [x] Examples match the public extension methods in the source tree.
