# ADR 002: Separate logical Runs from physical Attempts

- Status: Accepted
- Date: 2026-07-23

## Context

A distributed worker can crash, lose its lease, or lose the completion response.
Treating every retry as a new job hides execution history and makes cancellation,
idempotency, and stale-worker rejection ambiguous.

## Decision

A submitted business request creates one durable `JobRun`. Every physical
execution creates a child `JobAttempt` with a monotonically increasing attempt
number.

KubeJob explicitly guarantees at-least-once execution, not exactly-once external
side effects.

## Consequences

- Retry does not create another logical Run.
- Attempt history records each worker/session and outcome.
- Dashboard and APIs can explain lease loss and reassignment.
- Handlers interacting with external systems still need domain idempotency.
- Terminal Run state is accepted only from the current fenced Attempt.
