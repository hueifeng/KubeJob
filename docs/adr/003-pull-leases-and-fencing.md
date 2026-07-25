# ADR 003: Use pull scheduling, expiring leases, and fencing

- Status: Accepted
- Date: 2026-07-23

## Context

A push scheduler depends on current knowledge of every worker and can over-assign
work when control-plane replicas race or node state is stale. Network partitions
also allow an old process to continue after a replacement starts.

## Decision

Workers pull only when they have local free slots. The state store recomputes
capacity, creates a physical Attempt, and grants an expiring lease in one
transaction.

Completion and renewal require RunId, AttemptId, AttemptNumber, WorkerId,
SessionId, SessionEpoch, LeaseToken, current-Attempt identity, and an unexpired
lease. Worker registration and completion serialize on WorkerId in PostgreSQL.

## Consequences

- Multiple control-plane replicas require no in-memory leader for job claims.
- Stale or restarted workers cannot overwrite a newer Session.
- Lease expiration is the recovery mechanism for process and network failure.
- Worker memory and assignment are bounded by configured concurrency.
- A long handler must renew before its lease expires.
