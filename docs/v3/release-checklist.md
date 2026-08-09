# KubeJob V3 Release Checklist

## Runtime model

KubeJob V3 supports two execution authorities:

- **PostgresManaged**: PostgreSQL is the source of truth for Run, Attempt, lease and completion state.
- **BrokerNative**: Message broker delivery is the source of truth for transport delivery, acknowledgement and dead-letter handling.

The two models solve different problems and should not be mixed in the hot path.

## Architecture checks

- [x] Runtime does not depend on a specific message broker implementation.
- [x] BrokerNative execution does not require managed Run/lease database writes.
- [x] Transport adapters own broker-specific behavior.
- [x] Benchmark projects remain isolated from runtime libraries.
- [x] Remote and in-process submission both route through the configured queue authority.

## Cleanup checks

Remove before release:

- obsolete runtime dispatch abstractions
- unused compatibility shims
- temporary benchmark helpers
- debug-only code paths

## Documentation

Required documents:

- [x] architecture overview
- [x] runtime selection guide
- [x] event subscription model
- [x] transport capability model
- [x] benchmark methodology
