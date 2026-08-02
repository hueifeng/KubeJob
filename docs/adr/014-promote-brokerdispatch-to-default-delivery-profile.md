# ADR 014: Promote BrokerDispatch to the default execution delivery profile

## Status

Accepted (2026-07-31). Relates to [ADR 007](007-mq-notifications-do-not-own-jobs.md)
(MQ notifications do not own jobs), [ADR 011](011-hide-physical-delivery-from-job-submitters.md)
(hide physical delivery from job submitters), and
[ADR 013](013-defer-broker-authoritative-directpublish-mode.md) (defer
broker-authoritative DirectPublish mode).

## Context

Under the `Pull` delivery profile, every Worker claim happens via periodic
polling of `IJobClaimStore.ClaimAsync`'s `FOR UPDATE SKIP LOCKED` scan, even
when no work is pending. RabbitMQ notifications (ADR 007) only shorten the gap
between a Run becoming claimable and the next poll; they do not reduce the
number of empty poll round-trips PostgreSQL absorbs under load. PostgreSQL
ends up serving as state-store, queue, and lock manager at once.

`ExecutionDeliveryProfile.BrokerDispatch` already exists as a more complete
profile: the Outbox publishes a full `ExecutionEnvelope` to a per-consumer-
group RabbitMQ topology, and delivery calls `WorkerControlPlane.AdmitAsync`,
which invokes the same `ClaimAsync` used by `Pull` but scoped to one `RunId`
instead of an unscoped periodic scan. PostgreSQL remains the sole fencing,
idempotency, retry-budget, and `ConcurrencyKey` authority in both profiles —
`BrokerDispatch` changes only how a claimable Run is discovered, not who owns
its state. This keeps the promotion inside ADR 013's boundary (no Run-less,
broker-authoritative mode) and fulfills, rather than contradicts, ADR 011's
Direct Dispatch aspiration of hiding physical delivery behind a stable logical
`Queue`.

The blocker to promoting `BrokerDispatch` from opt-in to default was not code
coupling — flipping `QueueDeliveryOptions.Defaults.Profile` is mechanically a
one-line default change. The blocker was that `BrokerDispatch` had zero
automated test coverage against a real broker end-to-end (submit → outbox →
execution exchange → consumer → `AdmitAsync` → complete → ACK). The only
real-broker RabbitMQ integration tests exercised the unrelated business-
ingress adapter. Promoting an untested path to the default for all hosts was
not acceptable, so closing that gap was a precondition of this decision, not
follow-up polish.

## Decision

1. Added `RabbitMqExecutionDispatchIntegrationTests` (happy path, broker-level
   retry-then-succeed, and malformed-envelope reject-to-DLQ) exercising the
   full `BrokerDispatch` path against a real broker, closing the coverage gap.
2. Changed `QueueDeliveryOptions.Defaults.Profile` to
   `ExecutionDeliveryProfile.BrokerDispatch` with `Defaults.TransportId =
   "rabbitmq"`. This is a config default only: per-queue `QueueProfiles`
   overrides and `ConfigurationQueueRouter.Resolve`'s fallback logic are
   unchanged, so an operator can still pin any individual queue back to `Pull`.
   A host that does not register an `IExecutionTransport` (via the RabbitMQ
   execution dispatcher extensions) and does not opt back into `Pull` now hits
   `UnconfiguredExecutionTransport`'s existing clear failure at dispatch time
   instead of silently defaulting to a working profile.
3. Changed `JobRuntimeOptions.BrokerCancelPropagationEnabled` default to
   `true`. Under `Pull`-as-default this flag defaulted to `false`, so
   cancelling a `BrokerDispatch` Run relied on the lease-reaper/renewal loop
   rather than low-latency propagation — inconsistent once `BrokerDispatch`
   is the primary path. A host that registers the RabbitMQ execution
   dispatcher extensions (now the default expectation) also registers the
   `ICancelPublisher` this flag requires; a host that intentionally stays on
   `Pull` and skips those extensions must set it back to `false`.
4. Left `WorkerRuntimeService.ClaimLoopAsync` unconditional. It is the
   liveness floor when the broker is unavailable (ADR 007) and must keep
   running regardless of profile. Documented that BrokerDispatch-only
   deployments should raise `KubeJobWorkerOptions.EmptyPollDelay` (default 1s)
   since this loop is now pure fallback/reaper rather than the primary claim
   path in steady state.

## Consequences

- New hosts get broker-scoped, targeted admission by default instead of
  unscoped periodic scanning, without any change to fencing, idempotency, or
  retry semantics — the state machine and its guarantees are identical to
  `Pull`.
- A host that omits the RabbitMQ execution extensions now fails fast via
  `UnconfiguredExecutionTransport` instead of silently running `BrokerDispatch`
  against nothing; such hosts must explicitly set `Defaults.Profile = Pull` (or
  register the extensions).
- Cancellation of `BrokerDispatch` Runs is now low-latency by default; hosts
  without an `ICancelPublisher` must explicitly disable
  `BrokerCancelPropagationEnabled`.
- `docs/v2/message-transport.md` and `docs/v2/logical-architecture.md` are
  updated to describe these as the new defaults, with the new integration
  tests as the coverage evidence.
- This is promotion of the existing Direct Dispatch profile to be the default,
  not adoption of the deferred DirectPublish mode (ADR 013): PostgreSQL
  remains the sole authority for Run/Attempt/Lease/Outbox state in every
  profile, satisfying ADR 007's requirement that "a future full broker
  transport must be a separate design and cannot silently replace this state
  model" — no new state model was introduced, only a new default for an
  existing one.

## Rejected alternatives

### Keep `Pull` as the default and document `BrokerDispatch` as opt-in only

Rejected. Every default deployment would keep paying the periodic full-scan
cost that `BrokerDispatch` already solves, for no correctness benefit — the
two profiles share the same state machine and guarantees, so there is no
safety reason to keep the less scalable one as default once it has equivalent
test coverage.

### Promote the default without adding end-to-end broker test coverage first

Rejected. `BrokerDispatch` had no automated coverage against a real broker.
Making it the default for every new host without first proving the dispatch,
retry-reconciliation, and reject-to-DLQ paths work end-to-end would have
promoted an unverified path into the common case.
