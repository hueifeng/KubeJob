# KubeJob Domain Context

## Purpose

KubeJob accepts logical background-work requests, assigns them to capable
Workers, records physical execution attempts, and exposes durable progress to
applications and operators. It provides at-least-once execution and durable
state; it does not provide exactly-once external side effects.

## Terms

### Job key

A stable identifier for an executable job kind, such as `order-push-2`.
It identifies the handler capability, not one execution.

### Job run

One logical request to execute a Job key with a payload. A Run survives retries
and is the unit shown to callers and operators.

### Job attempt

One physical execution of a Run. A retry creates another Attempt under the
same Run; it does not create another logical request.

### Worker identity and session

Worker identity names a stable deployment or machine. A Session names one
process lifetime for that Worker. A newer Session fences an older one.

### Execution lease

A time-bounded authority for one Worker Session to execute one Attempt. A lease
is valid only with the matching Worker, Session, Epoch, Attempt, and token.

### Queue

A logical routing and capacity pool, such as `orders.push`. A Worker declares
which logical Queues it serves; a Run is eligible only for a matching Worker.
The physical delivery mechanism behind a Queue is an infrastructure concern
and is not part of a user's Run request.

### Delivery profile

An internal platform decision that maps a logical Queue to Pull or broker
dispatch. Users do not select it per Run. The platform may choose it from
deployment topology, backlog, capacity, and health.

### Concurrency key

A business serialization key, such as `order:O-1001`. Runs with the same key
must not execute concurrently, while different keys may execute in parallel.

### Ingress message

An external business message that requests creation of a Run. Its source and
stable message identity form the idempotency identity for acceptance.

### Dispatch envelope

A transport message that asks a Worker to perform an already accepted Run or
Attempt. It is a delivery carrier, not the authoritative job state.

### Wake-up signal

A best-effort hint that a Queue may have claimable work. It never grants
execution authority and may be duplicated, delayed, or lost.

### Schedule occurrence

One deterministic firing of a recurring Schedule. It creates at most one Run
for its Schedule and scheduled time.

## Invariants

1. One accepted idempotency identity creates at most one logical Run.
2. A Run has at most one current active Attempt.
3. Only the current, unexpired Execution lease can mutate an Attempt.
4. Broker acknowledgement follows durable acceptance or durable completion,
   according to the adapter contract.
5. Duplicate delivery is safe; external handlers must provide business
   idempotency for side effects.
6. Schedule cursor movement and occurrence Run creation are one durable state
   transition.
