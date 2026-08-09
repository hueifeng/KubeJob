# KubeJob documentation

Start with the [repository README](../README.md) if you have not run KubeJob
before. These pages describe the current V3 runtime; they are maintained with
the code and are the contract for new integrations.

## Guides

- [Local development](v3/local-development.md) — start PostgreSQL and
  RabbitMQ, run the sample, and execute tests.
- [Runtime model](v3/runtime-model.md) — choose between PostgresManaged and
  BrokerNative.
- [Event subscriptions](v3/events.md) — topics, subscription names, retries,
  and dead letters.
- [Transport and capabilities](v3/transport.md) — adapter boundaries and
  feature checks.
- [Benchmarking](v3/benchmarking.md) — reproduce and report throughput tests.

The [release checklist](v3/release-checklist.md) is for maintainers preparing
a release, not a user tutorial.

Contributors can use the [documentation notes](v3/documentation-style-references.md)
when adding or revising a guide.
