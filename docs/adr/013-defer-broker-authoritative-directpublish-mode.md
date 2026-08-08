# ADR 013: Defer broker-authoritative DirectPublish execution mode

## Status

Superseded (2026-08-08) by [ADR 015](015-single-authority-runtime-modes.md).

This ADR records the earlier decision made on 2026-07-26 to defer a Run-less,
broker-authoritative mode. V3 deliberately revisits that decision and adopts an
explicit `BrokerNative` runtime alongside `PostgresManaged`; the historical
reasoning below is retained for context only.

Relates to [ADR 011](011-hide-physical-delivery-from-job-submitters.md)
(BrokerDispatch delivery profile) and [ADR 012](012-bound-terminal-record-growth-with-per-job-retention.md)
(terminal retention).

## Context

The data-volume concern for high-throughput execution (ADR 012) raises a
further question: for code-driven high-TPS jobs, should we shed the `JobRun`
entirely — the business publishes straight to the execution broker, and KubeJob
hosts only the typed handler? This "DirectPublish" mode would record no `JobRun`
(at most a bounded metrics/failure log) and let the broker own delivery, retry,
and idempotency.

It is technically feasible without gutting the state machine: `JobHandlerRegistry`
(`TryGet(jobKey)` → `IJobHandlerInvoker.InvokeAsync`) is a separable seam, so a
new consumer adapter could invoke the handler directly, bypassing
`AdmitAsync`/`ClaimAsync` and the Run/Attempt/Lease path. The question is not
*can* we, but *should* we.

## Decision

Defer DirectPublish. Serve high-TPS code/ingress workloads with BrokerDispatch +
bounded per-job retention (ADR 012). Do not build a broker-authoritative,
Run-less execution mode in KubeJob now.

### Reasoning

1. **Narrow need.** BrokerDispatch live writes are bounded by `TPS × run
   lifetime` (~1s), not `TPS × retention`. In the realistic high-TPS band
   (1k–10k), live PostgreSQL writes are manageable; bounded retention solves
   storage. Only the extreme tail (50k+) genuinely needs to bypass the ledger,
   and at that scale teams run bespoke pipelines anyway.
2. **Product identity coherence.** Every existing ADR (004, 008, 010, 011)
   holds that KubeJob owns the durable state and the broker is
   delivery/notification, never the authority. DirectPublish inverts that for a
   slice of jobs, importing a second consistency model — a recurring tax on
   users (which mode? what guarantees differ? can I migrate?) and maintainers
   (two operational models, two failure-mode sets, two runbooks).
3. **Thin marginal value.** The value over a plain RabbitMQ consumer plus the
   user's own handler is handler-hosting consistency and shared infra — real
   but thin, and achievable without KubeJob owning the mode.
4. **Footgun and migration cost.** DirectPublish silently drops idempotency
   dedup, retry budget, fencing, cancellation, and `ConcurrencyKey`. A user who
   picks it for throughput and later needs one of those hits a wall and must
   migrate modes (different authority is a non-trivial migration).
   BrokerDispatch + retention is a graceful slope — the same guarantees, just
   tune retention — so needs can evolve without a mode migration.
5. **Reversibility.** Not building now is reversible: if concrete extreme-TPS
   demand repeats, DirectPublish can be added later without breaking existing
   modes. Building now and later regretting the dual-model tax is harder to
   reverse once users depend on it.

## Revisit criteria

Reopen this decision only if **all** of the following hold:

- A concrete, repeated workload exceeds ~10k TPS sustained and wants KubeJob's
  handler host (not a bespoke pipeline);
- the downstream is idempotent on its own key (no KubeJob dedup needed);
- bounded-retention BrokerDispatch is measurably the PostgreSQL bottleneck on
  **live write rate** (not storage);
- cancellation, `ConcurrencyKey`, and per-run observability are confirmed
  unneeded for that workload.

## Consequences

- The high-TPS path is single-mode (BrokerDispatch + retention): one
  consistency model, one operational runbook, one failure-mode set.
- The data-volume concern is solved by retention (ADR 012) for the realistic
  band.
- Users with broker-authoritative workloads use a plain consumer; KubeJob stays
  out of that tier.
- If the revisit criteria trigger, a future ADR can adopt DirectPublish
  additively.

## Rejected alternatives

### Build DirectPublish now

Rejected per the reasoning above: narrow need, coherence cost, footgun and
migration cost, and thin marginal value.

### Build DirectPublish as a degraded flag on BrokerDispatch

Rejected. A flag that silently drops guarantees is a footgun, and the two models
have different authorities and should not share a code path behind a boolean. If
DirectPublish is later adopted, it should be a distinct adapter with an explicit,
documented contract — not a flag on BrokerDispatch.
